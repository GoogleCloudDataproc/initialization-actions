#https://github.com/dask/dask-yarn/issues/101#issuecomment-539529524
import skein

spec = skein.ApplicationSpec.from_yaml("""
name: debug-skein
queue: root

master:
  script: echo "Things worked!"
""")

client = skein.Client()
client.submit(spec)
