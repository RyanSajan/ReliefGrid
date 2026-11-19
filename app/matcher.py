import asyncio
import json
import logging
import os

import aio_pika
from fastapi import HTTPException

from .common import call

logger = logging.getLogger(__name__)
COORD = os.getenv('COORDINATOR_URL', 'http://coordinator:8000')
MQ = os.getenv('RABBITMQ_URL', 'amqp://guest:guest@rabbitmq/')


async def main():
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(MQ, timeout=10, fail_fast=False)
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=10)
        queue = await channel.declare_queue('match_requests', durable=True)

        async def handler(msg):
            async with msg.process(requeue=True):
                try:
                    request_id = json.loads(msg.body)['request_id']
                    if not isinstance(request_id, str):
                        raise ValueError('invalid request ID')
                except (ValueError, KeyError, TypeError):
                    logger.warning('Discarding malformed matching message')
                    return
                try:
                    response = await call('POST', f'{COORD}/internal/claim/{request_id}', timeout=30)
                    if response.status_code in (200, 404):
                        return
                except HTTPException:
                    pass
                await asyncio.sleep(2)
                raise RuntimeError('Matching temporarily unavailable')

        await queue.consume(handler)
        await asyncio.Future()


if __name__ == '__main__':
    asyncio.run(main())

