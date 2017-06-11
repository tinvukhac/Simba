import matplotlib.pyplot as plt
from shapely.geometry.polygon import Polygon


class MBR:
    mbr = Polygon()

    def __init__(self, data):
        y1, x1, y2, x2 = float(data[0]), float(data[1]), float(data[2]), float(data[3])
        self.mbr = Polygon([(x1, y1), (x1, y2), (x2, y2), (x2, y1)])


def plot_points(pointsfile):
    with open(pointsfile) as f:
        lines = f.readlines()
        x = []
        y = []
        for line in lines:
            data = line.replace('(', '').replace(')', '').split(",")
            x.append(float(data[1]))
            y.append(float(data[0]))
        plt.scatter(x, y)

    plt.savefig("center_points.png")


def plot_mbrs(filename):
    with open(filename) as f:
        fig = plt.figure(1, figsize=(50, 50), dpi=90)
        ax = fig.add_subplot(111)
        lines = f.readlines()
        for line in lines:
            print line
            data = line.split(",")
            p = MBR(data[0:4])
            x, y = p.mbr.exterior.xy
            ax.plot(x, y, color='#ff0000', alpha=1.0, linewidth=1, solid_capstyle='round', zorder=2)
        plt.savefig("gridcells.png")


def plot_mbrs_points(mbrsfile, pointsfile):
    fig = plt.figure(1, figsize=(50, 50), dpi=90)
    with open(mbrsfile) as f:
        ax = fig.add_subplot(111)
        lines = f.readlines()
        for line in lines:
            data = line.split(",")
            p = MBR(data[0:4])
            x, y = p.mbr.exterior.xy
            ax.plot(x, y, color='#ff0000', alpha=1.0, linewidth=1, solid_capstyle='round', zorder=2)

    with open(pointsfile) as f:
        lines = f.readlines()
        x = []
        y = []
        for line in lines:
            data = line.split(",")
            x.append(data[1])
            y.append(data[0])
        plt.scatter(x, y)

    plt.savefig("mbr.png")


def main():
    # plot_mbrs_points("mbrs.txt", "points.txt")
    # plot_mbrs("gridcells.txt")
    plot_points("center_points.txt")


if __name__ == "__main__":
    main()