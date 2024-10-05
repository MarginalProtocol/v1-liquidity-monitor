import os
import pandas as pd

from typing import Annotated

from eth_abi.packed import encode_packed
from eth_utils import keccak

from ape import chain, Contract
from ape.logging import logger, LogLevel
from ape.api import BlockAPI
from taskiq import Context, TaskiqDepends, TaskiqState

from telegram import Bot
from silverback import AppState, SilverbackApp

# Do this first to initialize your app
app = SilverbackApp()

# Marginal v1 pool contract
pool = Contract(os.environ["CONTRACT_ADDRESS_MARGV1_POOL"])

# Telegram bot variables
# TODO: abstact away in messenger client class
_bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
bot = Bot(token=_bot_token) if _bot_token else None
channel = os.environ.get("TELEGRAM_CHANNEL_ID", "")


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
    event_name: str,
    position_owner: str,
    position_id: int,
    liquidity_before: int,
    liquidity_after: int,
):
    """
    Handles a position close event of type Settle or Liquidate.

    Checks pool invariant `liquidityReturned >= position.liquidityLocked (owed)` holds
    on position liquidation or settlement to verify LPs do not experience bad debt.

    Alerts via message if invariant broken (should only occur in extreme funding cases).
    """
    k = get_position_key(position_owner, position_id)
    position = pool.positions(k, block_identifier=block_number - 1)
    liquidity_returned = liquidity_after - liquidity_before

    logger.info(f"Position liquidity locked: {position.liquidityLocked}")
    logger.info(f"Position liquidity returned on close: {liquidity_returned}")

    msg = (
        f"🚨 Pool {event_name} on position {position_id} at block number {block_number} lost liquidity. liquidity_returned / position.liquidityLocked: {liquidity_returned / position.liquidityLocked} 🚨"
        if liquidity_returned < position.liquidityLocked
        else f"Pool {event_name} on position {position_id} at block number {block_number} gained liquidity. liquidity_returned / position.liquidityLocked: {liquidity_returned / position.liquidityLocked}"
    )
    method = "error" if liquidity_returned < position.liquidityLocked else "success"
    getattr(logger, method)(msg)

    # send message to channel based on log level and liquidity gained/lost
    send = (
        liquidity_returned >= position.liquidityLocked
        and logger.level <= LogLevel.SUCCESS
    ) or (
        liquidity_returned < position.liquidityLocked and logger.level <= LogLevel.ERROR
    )
    if bot and send:
        bot.send_message(channel_id=channel, text=msg)


# This is how we trigger off of new blocks
@app.on_(chain.blocks)
# NOTE: The type hint for block is `BlockAPI`, but we parse it using `EcosystemAPI`
# NOTE: If you need something from worker state, you have to use taskiq context
def exec_block(block: BlockAPI, context: Annotated[Context, TaskiqDepends()]):
    logger.info(f"Block found with block number: {block.number}")
    # cache pool liquidity, liquidityLocked values from end of prior block to use as ref in events
    # @dev pool state for eth_call will be values after all transactions executed in block
    liquidity = pool.state(block_id=block.number - 1).liquidity
    logger.info(
        f"Pool liquidity at end of prior block with block number {block.number-1}: {liquidity}"
    )

    # handle any state changing events that have occured in this block
    events = [pool.Open, pool.Settle, pool.Liquidate, pool.Swap, pool.Mint, pool.Burn]
    logger.info(f"Querying for pool events in block number {block.number} ...")
    queries = [
        ev.query("*", start_block=block.number, stop_block=block.number)
        for ev in events
    ]
    df = pd.concat(queries)
    df = df.sort_values(["transaction_index", "log_index"])
    logger.info(f"Pool events found in block number {block.number}: {len(df)}")
    if not df.empty:
        logger.info(f"Pool events in block number {block.number}: {df}")

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
                block.number,
                row.event_name,
                ev_args["owner"],
                ev_args["id"],
                liquidity,
                liquidity_after,
            )
            count += 1

        # update cached liquidity for next event in block
        liquidity = liquidity_after

    logger.info(f"Total positions closed in block number {block.number}: {count}")
    return count


# A final job to execute on Silverback shutdown
@app.on_shutdown()
def app_shutdown():
    # raise Exception  # NOTE: Any exception raised on shutdown is ignored
    return {"some_metric": 123}


# Just in case you need to release some resources or something inside each worker
@app.on_worker_shutdown()
def worker_shutdown(state: TaskiqState):  # NOTE: You need the type hint here
    return
    # raise Exception  # NOTE: Any exception raised on worker shutdown is ignored
