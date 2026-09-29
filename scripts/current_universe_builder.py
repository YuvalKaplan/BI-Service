import atexit
from modules.object.exit import cleanup
from modules.cron import universe_builder

atexit.register(cleanup)

if __name__ == '__main__':
    stats = universe_builder.run()
    print(universe_builder.summary(stats))
    for line in stats.non_equity:
        print(f"  note/preferred line left out: {line}")
    for line in stats.duplicate_companies:
        print(f"  duplicate dropped: {line}")
    for line in stats.foreign_admitted:
        print(f"  foreign line admitted: {line}")
