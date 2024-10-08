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
chat = os.environ["TELEGRAM_CHAT_ID"] if bot else None


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


async def attempt_send_message(msg: str, level: int):
    """
    Attempts to send message if logger.level > level
    """
    if bot is None or logger.level > level:
        return
    await bot.send_message(chat_id=chat, text=msg)


async def handle_position_close(
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
        else f"✅ Pool {event_name} on position {position_id} at block number {block_number} gained liquidity. liquidity_returned / position.liquidityLocked: {liquidity_returned / position.liquidityLocked} ✅"
    )
    method = "error" if liquidity_returned < position.liquidityLocked else "success"
    getattr(logger, method)(msg)

    # send message to chat based on log level and liquidity gained/lost
    level = (
        LogLevel.ERROR
        if liquidity_returned < position.liquidityLocked
        else LogLevel.SUCCESS
    )
    await attempt_send_message(msg, level)


# This is how we trigger off of new blocks
@app.on_(chain.blocks)
# NOTE: The type hint for block is `BlockAPI`, but we parse it using `EcosystemAPI`
# NOTE: If you need something from worker state, you have to use taskiq context
async def exec_block(block: BlockAPI, context: Annotated[Context, TaskiqDepends()]):
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
    counts = {"Open": 0, "Settle": 0, "Liquidate": 0, "Swap": 0, "Mint": 0, "Burn": 0}
    liquidity_before = liquidity
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
            await handle_position_close(
                block.number,
                row.event_name,
                ev_args["owner"],
                ev_args["id"],
                liquidity,
                liquidity_after,
            )

        # increment counts
        counts[row.event_name] += 1

        # update cached liquidity for next event in block
        liquidity = liquidity_after

    opened = counts["Open"]
    closed = counts["Settle"] + counts["Liquidate"]
    logger.info(f"Total positions closed in block number {block.number}: {closed}")

    # summary for block
    msg = f"⛓️ Block found with block number {block.number} ....\n\nPool liquidity (start): {liquidity_before}\nPool liquidity (end): {liquidity}\nPool liquidity delta: {liquidity - liquidity_before}\n\nPositions opened: {opened}\nPositions closed: {closed}\n\nSwaps: {counts['Swap']}\nLiquidity adds: {counts['Mint']}\nLiquidity removes: {counts['Burn']} ⛓️"
    logger.info(msg)
    await attempt_send_message(msg, LogLevel.INFO)
    return counts


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
