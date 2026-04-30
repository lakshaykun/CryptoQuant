# generate a migration

alembic revision --autogenerate -m "create market table"

# apply migrations

alembic upgrade head

# rollback

alembic downgrade -1
