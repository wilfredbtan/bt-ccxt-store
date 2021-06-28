from setuptools import setup

setup(
   name='bt_ccxt_store',
   version='1.0',
   description='A fork of Dave Vallance\'s fork of Ed Bartosh\'s CCXT Store Work with some additions',
   url='https://github.com/wilfredbtan/bt-ccxt-store',
   author='Wilfred Bradley Tan',
   author_email='wilfredbtan@gmail.com',
   license='MIT',
   packages=['ccxtbt'],  
   install_requires=['backtrader','ccxt'],
)
