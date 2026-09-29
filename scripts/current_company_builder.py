import atexit
from modules.object.exit import cleanup
from modules.cron import company_builder

atexit.register(cleanup)

if __name__ == '__main__':
    stats = company_builder.run()
    print(company_builder.summary(stats))
    for line in stats.non_equity:
        print(f"  note/preferred line left out: {line}")
    for line in stats.duplicate_companies:
        print(f"  duplicate dropped: {line}")
    for line in stats.foreign_admitted:
        print(f"  foreign line admitted: {line}")
