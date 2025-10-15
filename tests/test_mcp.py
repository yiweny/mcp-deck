import asyncio
from mcp.client.sse import sse_client
from mcp import ClientSession

async def main():
    async with sse_client("http://localhost:8080/sse") as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            try:
                await session.initialize()
                result = await session.list_tools()
                print(result)
                result = await session.call_tool("user_purchase_history", arguments={"user_id": "2435b0c7d464e295b87a97e2f1a3831e699535eae4746fb2e89af0936471502c"})
                print(result)
            except Exception as e:
                print(f"Error: {e}")
asyncio.run(main())