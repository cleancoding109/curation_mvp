-- Reference table: adjuster_lookup
-- This table should be created directly in the SDL schema with reference data

CREATE TABLE IF NOT EXISTS ltc_insurance.standardized_data_layer.adjuster_lookup (
  adjuster_id STRING,
  adjuster_name STRING,
  adjuster_region STRING,
  adjuster_level STRING,
  is_active BOOLEAN
);

-- Insert sample reference data
INSERT OVERWRITE ltc_insurance.standardized_data_layer.adjuster_lookup VALUES
  ('ADJ001', 'John Smith', 'Northeast', 'Senior', true),
  ('ADJ002', 'Sarah Johnson', 'Southeast', 'Senior', true),
  ('ADJ003', 'Michael Brown', 'Midwest', 'Junior', true),
  ('ADJ004', 'Emily Davis', 'West', 'Senior', true),
  ('ADJ005', 'Robert Wilson', 'Northeast', 'Lead', true),
  ('ADJ006', 'Jennifer Martinez', 'Southeast', 'Junior', true),
  ('ADJ007', 'David Anderson', 'Midwest', 'Senior', false),
  ('ADJ008', 'Lisa Thomas', 'West', 'Lead', true),
  ('ADJ009', 'James Taylor', 'Northeast', 'Junior', true),
  ('ADJ010', 'Patricia Moore', 'Southeast', 'Senior', true);
