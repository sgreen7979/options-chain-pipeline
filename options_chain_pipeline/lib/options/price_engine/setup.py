from setuptools import setup, Extension

binomialtree_module = Extension('binomialtree', sources=['binomialtree.c'])
blackscholes_module = Extension('blackscholes', sources=['black_scholes.c'])

setup(
    name='PricingEngines',
    version='1.0',
    description='Pricing Engines for American and European Options',
    ext_modules=[binomialtree_module, blackscholes_module],
)
