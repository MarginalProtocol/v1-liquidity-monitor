# v1-liquidity-monitor

Bot to monitor pool liquidity for Marginal v1. Alerts when the invariant

```python
liquidityReturned >= position.liquidityLocked  # (owed)
```

is broken for position settlement or liquidation. Should only occur in extreme
funding payment cases, as LPs should not experience bad debt.

## Installation

The repo uses [ApeWorX](https://github.com/apeworx/ape) for development and [uv](https://github.com/astral-sh/uv) for project management.

Install requirements and Ape plugins

```sh
uv sync
uv run ape plugins install .
```

## Usage

Include environment variables for the address of the [`MarginalV1Pool`](https://github.com/MarginalProtocol/book/blob/main/src/v1/core/contracts/MarginalV1Pool.sol/contract.MarginalV1Pool.md) contract verified on the network

```sh
export CONTRACT_ADDRESS_MARGV1_POOL=<address of marginal v1 pool contract on network>
export TELEGRAM_BOT_TOKEN=<telegram bot token>
export TELEGRAM_CHANNEL_ID=<telegram channel ID to send messages to>
```

Then run silverback

```sh
uv run silverback run "main:app" --network :mainnet:alchemy
```
