if __name__ == "__main__":
    import argparse

    aparser = argparse.ArgumentParser(description='Detect sequential streams.')
    aparser.add_argument('--trace', required=True, dest='trace', help='path to json trace file')
    aparser.add_argument('--csv', required=False, dest='csv', help='path to csv output file')
    aparser.add_argument('--plot', required=False, dest='plot', help='path to plot output file')

    args = aparser.parse_args()

    from json_reader import JsonReader
    from sata import Parser
    from trace_statistics import commandsToStats, commandsToStatCSV, Time, LBA, Length, Stream
    from sequential_stream import Detector
    from itertools import tee, zip_longest

    commands = Parser(JsonReader(args.trace))
    commands = Detector(commands, attrName='stream1', tDelta=1)
    commands = Detector(commands, attrName='stream10', tDelta=10)
    commands = Detector(commands, attrName='stream100', tDelta=100)
    commands = Detector(commands, attrName='stream1000', tDelta=1000)
    commands = Detector(commands, attrName='stream10000', tDelta=10000)

    outputs = []

    statFields = [Time(True), LBA(), Length(), Stream('stream1'), Stream('stream10'), Stream('stream100'), Stream('stream1000'), Stream('stream10000')]

    if args.csv is not None:
        outputs.append(lambda cmds: commandsToStatCSV(args.csv, cmds, fields=statFields))

    import matplotlib.pyplot as plt
    if args.plot is not None:
        def plotOutputter(cmds):
            from functools import reduce
            from collections import defaultdict
            from numpy import fromiter
            def plot(stats, key, subplot):
                def accumulator(accum, s):
                    accum[s[key]].append(s['Start Time'])
                    return accum
                streams = reduce(accumulator, stats, defaultdict(list))
                plt.subplot(subplot)
                for i, t in streams.items():
                    plt.plot(t, [i for _ in range(len(t))], '.')
            stats = tee(commandsToStats(cmds, fields=statFields), 5)
            plot(stats[0], 'stream1', 321)
            plot(stats[1], 'stream10', 322)
            plot(stats[2], 'stream100', 323)
            plot(stats[3], 'stream1000', 324)
            plot(stats[4], 'stream10000', 325)
        outputs.append(plotOutputter)

    for t in zip_longest(outputs, tee(commands, len(outputs))):
        t[0](t[1])

    if args.plot is not None:
        plt.savefig(args.plot)
