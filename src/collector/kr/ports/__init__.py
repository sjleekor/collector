"""Port interfaces (driven / driving) for the KRX data pipeline.

Ports define the contracts that adapters must implement. They use
``typing.Protocol`` so that adapters are structurally (duck-) typed and
the domain layer stays free of infrastructure imports.
"""
