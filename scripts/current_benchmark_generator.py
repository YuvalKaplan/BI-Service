import atexit
from modules.object.exit import cleanup
from modules.cron import benchmark_generator

atexit.register(cleanup)

if __name__ == '__main__':
    stats = benchmark_generator.run()
    print(benchmark_generator.summary(stats))
