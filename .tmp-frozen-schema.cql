-- Create source keyspace
CREATE KEYSPACE IF NOT EXISTS frozen_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Create UDTs
CREATE TYPE IF NOT EXISTS frozen_test.address_type (street text, city text, zip int, country text);
CREATE TYPE IF NOT EXISTS frozen_test.phone_type (country_code int, number text, extension text);
CREATE TYPE IF NOT EXISTS frozen_test.contact_info (name text, email text, phone frozen<frozen_test.phone_type>, address frozen<frozen_test.address_type>);
CREATE TYPE IF NOT EXISTS frozen_test.order_item (product_id uuid, qty int, price decimal, tags set<text>);
CREATE TYPE IF NOT EXISTS frozen_test.shipping_info (carrier text, tracking_id text, address frozen<frozen_test.address_type>, estimated_delivery timestamp);

-- Table 1: frozen UDT column
CREATE TABLE IF NOT EXISTS frozen_test.customers (
  id uuid PRIMARY KEY,
  name text,
  contact frozen<frozen_test.contact_info>,
  created_at timestamp
);

-- Table 2: list of frozen UDTs
CREATE TABLE IF NOT EXISTS frozen_test.orders (
  id uuid PRIMARY KEY,
  customer_id uuid,
  items list<frozen<frozen_test.order_item>>,
  shipping frozen<frozen_test.shipping_info>,
  total decimal,
  status text
);

-- Table 3: map with frozen UDT value
CREATE TABLE IF NOT EXISTS frozen_test.warehouses (
  id uuid PRIMARY KEY,
  name text,
  locations map<text, frozen<frozen_test.address_type>>,
  manager frozen<frozen_test.contact_info>
);

-- Table 4: set of frozen UDTs
CREATE TABLE IF NOT EXISTS frozen_test.suppliers (
  id uuid PRIMARY KEY,
  company text,
  contacts set<frozen<frozen_test.phone_type>>,
  hq_address frozen<frozen_test.address_type>
);

-- Table 5: multiple frozen columns
CREATE TABLE IF NOT EXISTS frozen_test.shipments (
  id uuid PRIMARY KEY,
  origin frozen<frozen_test.address_type>,
  destination frozen<frozen_test.address_type>,
  carrier_contact frozen<frozen_test.contact_info>,
  items list<frozen<frozen_test.order_item>>,
  status text,
  shipped_at timestamp
);
