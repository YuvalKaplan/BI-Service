import atexit
from modules.object.exit import cleanup
from modules.ticker import master

atexit.register(cleanup)

if __name__ == '__main__':
    masters_updated, caps_updated = master.sync_masters_and_accumulated_caps()
    print(f"Master sync: {masters_updated} link(s), {caps_updated} accumulated cap(s) refreshed")
