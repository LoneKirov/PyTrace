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

    if args.plot is not None:
        def plot(cmds):
            import matplotlib.pyplot as plt
            from numpy import fromiter
            stats = tee(filter(lambda s: s['stream1000'] != -1, commandsToStats(cmds, fields=statFields)))
            times = fromiter(map(lambda s: float(s['Start Time']), stats[0]), float)
            streams = fromiter(map(lambda s: int(s['stream1000']), stats[1]), int)
            print(plt.plot(times, streams, '.'))
            plt.savefig(args.plot)
        outputs.append(plot)

    for t in zip_longest(outputs, tee(commands, len(outputs))):
        t[0](t[1])
