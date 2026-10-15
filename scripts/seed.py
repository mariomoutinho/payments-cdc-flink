"""Deterministic domain data (seed=42); no Faker, network or personal information."""
import random

def generate_sql(seed=42):
    rng = random.Random(seed)
    def insert(table, columns, rows):
        def literal(v):
            return str(v) if isinstance(v, int) else "'" + v.replace("'", "''") + "'"
        return f"INSERT INTO {table} ({columns}) VALUES " + ','.join('(' + ','.join(map(literal, row)) + ')' for row in rows) + ';'
    customers = [(1001+i, f'Customer Synthetic {i:03d}', f'customer{i:03d}@example.invalid') for i in range(200)]
    accounts = [(1001+i, customers[i % 200][1], rng.choice(['low','medium','high'])) for i in range(240)]
    cards = [(f'seed-card-{i:03d}', accounts[i % 240][0]) for i in range(320)]
    merchants = [(1001+i, f'Merchant Synthetic {i:02d}') for i in range(60)]
    transactions = []
    for i in range(2000):
        card = cards[rng.randrange(len(cards))]
        transactions.append((10001+i, card[1], card[0], rng.randrange(1,1000000), 1735603200000+i*1000))
    return '\n'.join(['BEGIN;', 'SET LOCAL ROLE payments_writer;',
        insert('customers','customer_id,customer_name,email', customers),
        insert('accounts','account_id,customer_name,risk_level', accounts),
        insert('cards','card_id,account_id', cards),
        insert('merchants','merchant_id,merchant_name', merchants),
        insert('transactions','transaction_id,account_id,card_id,amount_cents,occurred_at', transactions), 'COMMIT;'])

if __name__ == '__main__':
    print(generate_sql())
