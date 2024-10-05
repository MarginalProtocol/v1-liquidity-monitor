import click
import os
import pandas as pd

from typing import Annotated, Dict

from eth_abi.packed import encode_packed
from eth_utils import keccak

from ape import chain, Contract
from ape.api import BlockAPI
from taskiq import Context, TaskiqDepends, TaskiqState

from silverback import AppState, SilverbackApp

# Do this first to initialize your app
app = SilverbackApp()

# Marginal v1 pool contract
pool = Contract(os.environ["CONTRACT_ADDRESS_MARGV1_POOL"])


# TODO: messenger client class


@app.on_startup()
def app_startup(startup_state: AppState):
    # NOTE: This is called just as the app is put into "run" state,
    #       and handled by the first available worker
    # raise Exception  # NOTE: Any exception raised on startup aborts immediately
    return {"message": "Starting...", "block_number": startup_state.last_block_seen}


@app.on_worker_startup()
def worker_startup(state: TaskiqState):  # NOTE: You need the type hint here
    # NOTE: Can put anything here, any python object works
    state.block_count = 0
    # raise Exception  # NOTE: Any exception raised on worker startup aborts immediately


def get_position_key(address: str, id: int) -> bytes:
    return keccak(encode_packed(["address", "uint96"], [address, id]))


def handle_position_close(
    block_number: int,
    ev_name: str,
    ev_args: Dict,
    liquidity_before: int,
    liquidity_after: int,
):
    """
    Handles a position close event of type Settle or Liquidate.

    Checks pool invariant `liquidityReturned >= position.liquidityLocked (owed)` holds
    on position liquidation or settlement to verify LPs do not experience bad debt.

    Alerts via message if invariant broken (should only occur in extreme funding cases).
    """
    k = get_position_key(ev_args["owner"], ev_args["id"])
    position = pool.positions(k, block_identifier=block_number - 1)
    liquidity_returned = liquidity_after - liquidity_before

    if liquidity_returned < position.liquidityLocked:
        liquidity_lost = position.liquidityLocked - liquidity_returned
        click.secho(
            f"Pool {ev_name} on position {ev_args['id']} at block number {block_number} lost liquidity: {liquidity_lost}",
            blink=True,
            bold=True,
        )
        # TODO: send alert through messenger


# This is how we trigger off of new blocks
@app.on_(chain.blocks)
# NOTE: The type hint for block is `BlockAPI`, but we parse it using `EcosystemAPI`
# NOTE: If you need something from worker state, you have to use taskiq context
def exec_block(block: BlockAPI, context: Annotated[Context, TaskiqDepends()]):
    click.echo(f"New block found with block number: {block.number}")
    # cache pool liquidity, liquidityLocked values from end of prior block to use as ref in events
    # @dev pool state for eth_call will be values after all transactions executed in block
    liquidity = pool.state(block_id=block.number - 1).liquidity
    click.echo(
        f"Pool liquidity at end of prior block with block number {block.number-1}: {liquidity}"
    )

    # handle any state changing events that have occured in this block
    events = [pool.Open, pool.Settle, pool.Liquidate, pool.Swap, pool.Mint, pool.Burn]
    queries = [
        ev.query("*", start_block=block.number, stop_block=block.number + 1)
        for ev in events
    ]
    df = pd.concat(queries)
    df = df.sort_values(["transaction_index", "log_index"])

    # loop through logs sorted by log index to reconstruct pool state changes within block
    count = 0
    for _, row in df.iterrows():
        ev_args = row.event_arguments
        if "liquidityAfter" in ev_args:
            # Open, Settle, Liquidate
            liquidity_after = ev_args["liquidityAfter"]
        elif "liquidity" in ev_args:
            # Swap
            liquidity_after = ev_args["liquidity"]
        elif "liquidityDelta" in ev_args:
            # Mint, Burn
            sign = 1 if row.event_name == "Mint" else -1
            liquidity_after = liquidity + sign * ev_args["liquidityDelta"]

        if row.event_name == "Settle" or row.event_name == "Liquidate":
            handle_position_close(
                block.number, row.event_name, ev_args, liquidity, liquidity_after
            )
            count += 1

        # update cached liquidity for next event in block
        liquidity = liquidity_after

    return count


# A final job to execute on Silverback shutdown
@app.on_shutdown()
def app_shutdown():
    # raise Exception  # NOTE: Any exception raised on shutdown is ignored
    return {"some_metric": 123}


# Just in case you need to release some resources or something inside each worker
@app.on_worker_shutdown()
def worker_shutdown(state: TaskiqState):  # NOTE: You need the type hint here
    pass
    # raise Exception  # NOTE: Any exception raised on worker shutdown is ignored
