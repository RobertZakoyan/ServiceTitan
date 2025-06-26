import pandas as pd
import pickle

class CustomerDataExtractor:
    CATEGORY_MAP = {
        1: 'Electronics',
        2: 'Apparel',
        3: 'Books',
        4: 'Home Goods'
    }

    def __init__(self, customer_file_path, vip_file_path):
        self.customer_file_path = customer_file_path
        self.vip_file_path = vip_file_path
        self.vip_ids = set()
        self.customer_data = []

    def load_data(self):
        with open(self.vip_file_path, 'r') as f:
            self.vip_ids = set(map(int, f.read().splitlines()))

        with open(self.customer_file_path, 'rb') as f:
            self.customer_data = pickle.load(f)

    def transform(self):
        rows = []

        for customer in self.customer_data:
            cid = int(customer['id'])
            is_vip = cid in self.vip_ids
            name = customer['name']

            try:
                reg_date = pd.to_datetime(customer['registration_date'])
            except Exception:
                continue

            for order in customer.get('orders', []):
                try:
                    oid = int(order['order_id'])
                    odate = pd.to_datetime(order['order_date'])
                except Exception:
                    continue

                total_order_val = 0
                item_entries = []

                for item in order.get('items', []):
                    if not all(k in item and item[k] is not None for k in ['item_id', 'product_name', 'category', 'price', 'quantity']):
                        continue
                    try:
                        price = float(str(item['price']).replace('$', '').replace(',', ''))
                        qty = int(item['quantity'])
                    except Exception:
                        continue

                    total_item_price = price * qty
                    total_order_val += total_item_price
                    item_entries.append((item, total_item_price))

                for item, total_item_price in item_entries:
                    category = self.CATEGORY_MAP.get(item.get('category'), 'Misc')

                    rows.append({
                        'customer_id': cid,
                        'customer_name': name,
                        'registration_date': reg_date,
                        'is_vip': is_vip,
                        'order_id': oid,
                        'order_date': odate,
                        'product_id': int(item['item_id']),
                        'product_name': item['product_name'],
                        'category': category,
                        'unit_price': float(str(item['price']).replace('$', '').replace(',', '')),
                        'item_quantity': int(item['quantity']),
                        'total_item_price': total_item_price,
                        'total_order_value_percentage': (total_item_price / total_order_val) if total_order_val else 0.0
                    })

        df = pd.DataFrame(rows)
        df = df.astype({
            'customer_id': 'int64',
            'customer_name': 'string',
            'registration_date': 'datetime64[ns]',
            'is_vip': 'bool',
            'order_id': 'int64',
            'order_date': 'datetime64[ns]',
            'product_id': 'int64',
            'product_name': 'string',
            'category': 'string',
            'unit_price': 'float',
            'item_quantity': 'int64',
            'total_item_price': 'float',
            'total_order_value_percentage': 'float'
        })
        df.sort_values(by=['customer_id', 'order_id', 'product_id'], inplace=True)
        return df

# === Run it ===
if __name__ == "__main__":
    extractor = CustomerDataExtractor("C:\\Users\\mafro\\Downloads\\customer_orders.pkl", "C:\\Users\\mafro\\Downloads\\vip_customers.txt")
    extractor.load_data()
    df = extractor.transform()
    df.to_csv("cleaned_customer_orders.csv", index=False)
    print("✅ DataFrame saved to cleaned_customer_orders.csv")