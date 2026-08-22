"""options/ — Indian index & stock options data layer.

Separate from broker/ on purpose. The broker adapters speak to crypto venues
and carry an order path; nothing here places an order. This package only
acquires, stores and audits historical options data, because the thing that
went wrong last time was not execution — it was believing a dataset that was
not there.
"""
