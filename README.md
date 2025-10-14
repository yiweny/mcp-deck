# mcp-deck

Generate schema

```
# Generate schema for articles
uv run scripts/generate_schema.py --input data/articles.parquet --output schemas/articles.json --pkey article_id 

# Generate schema for customers
uv run scripts/generate_schema.py --input data/customers.parquet --output schemas/customers.json --pkey customer_id 

# Generate schema for transactions
uv run scripts/generate_schema.py --input data/transactions.parquet --output schemas/transactions.json --fkeys articles_id=articles.article_id customer_id=customers.customer_id


```


Initialize the MCP server

```
uv run python -m mcp_deck.server
```


Run docker somewhere

```
docker run --name mcpdeck-mysql \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=mcpdeck \
  -p 3306:3306 -d mysql:8
```