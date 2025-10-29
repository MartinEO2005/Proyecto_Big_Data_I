import ee, os
try:
    ee.Initialize()
    print('ee.Initialize OK')
except Exception as e:
    print('ee.Initialize falló:', type(e).__name__, e)
