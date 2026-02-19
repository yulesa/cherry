# This example shows a simple pipeline that ingests and decodes ERC-20 transfers
# directly from an Ethereum RPC node (e.g. a local node or Alchemy/Infura endpoint).
# Cherry is published to PyPI as cherry-etl and cherry-core.
# To install it, run: pip install cherry-etl cherry-core
# Or with uv: uv pip install cherry-etl cherry-core

# You can run this script with:
# RPC_URL=https://mainnet.gateway.tenderly.co uv run examples/erc20_rpc.py

# After run, you can see the result in the database:
# duckdb data/rpc_transfers.db
# SELECT * FROM transfers LIMIT 3;

import asyncio
import logging
import os
from pathlib import Path

import duckdb
import pyarrow as pa
from dotenv import load_dotenv

from cherry_core import evm_signature_to_topic0, ingest
from cherry_etl import config as cc
from cherry_etl import run_pipeline

load_dotenv()

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO").upper())
logger = logging.getLogger(__name__)

DATA_PATH = str(Path.cwd() / "data")
Path(DATA_PATH).mkdir(parents=True, exist_ok=True)

FROM_BLOCK = 22_000_000
TO_BLOCK = 22_001_000

TRANSFER_SIGNATURE = "Transfer(address indexed from, address indexed to, uint256 amount)"


async def main():
    rpc_url = os.environ.get("RPC_URL", "http://localhost:8545")
    logger.info(f"using RPC endpoint: {rpc_url}")
    logger.info(f"ingesting blocks {FROM_BLOCK} to {TO_BLOCK}")

    provider = ingest.ProviderConfig(
        kind=ingest.ProviderKind.RPC,
        url=rpc_url,
        # stop once we reach the head of the requested range
        stop_on_head=True,
        rpc=ingest.RpcProviderConfig(
            # keep each eth_getLogs call within typical provider limits
            max_block_range=100,
        ),
    )

    query = ingest.Query(
        kind=ingest.QueryKind.EVM,
        params=ingest.evm.Query(
            from_block=FROM_BLOCK,
            to_block=TO_BLOCK,
            logs=[
                ingest.evm.LogRequest(
                    topic0=[evm_signature_to_topic0(TRANSFER_SIGNATURE)],
                )
            ],
            fields=ingest.evm.Fields(
                log=ingest.evm.LogFields(
                    block_number=True,
                    transaction_hash=True,
                    log_index=True,
                    address=True,
                    topic0=True,
                    topic1=True,
                    topic2=True,
                    topic3=True,
                    data=True,
                ),
            ),
        ),
    )

    connection = duckdb.connect("data/rpc_transfers.db")

    writer = cc.Writer(
        kind=cc.WriterKind.DUCKDB,
        config=cc.DuckdbWriterConfig(connection=connection.cursor()),
    )

    pipeline = cc.Pipeline(
        provider=provider,
        query=query,
        writer=writer,
        steps=[
            cc.Step(
                kind=cc.StepKind.EVM_DECODE_EVENTS,
                config=cc.EvmDecodeEventsConfig(
                    event_signature=TRANSFER_SIGNATURE,
                    output_table="transfers",
                    allow_decode_fail=True,
                ),
            ),
            cc.Step(
                kind=cc.StepKind.HEX_ENCODE,
                config=cc.HexEncodeConfig(),
            ),
            cc.Step(
                name="i256_to_i128",
                kind=cc.StepKind.CAST_BY_TYPE,
                config=cc.CastByTypeConfig(
                    from_type=pa.decimal256(76, 0),
                    to_type=pa.decimal128(38, 0),
                    allow_cast_fail=True,
                ),
            ),
        ],
    )

    await run_pipeline(pipeline=pipeline)

    result = connection.sql(
        'SELECT block_number, transaction_hash, "from", "to", amount'
        " FROM transfers ORDER BY block_number, log_index LIMIT 10"
    )
    logger.info(f"last 10 transfers:\n{result}")
    connection.close()


if __name__ == "__main__":
    asyncio.run(main())
