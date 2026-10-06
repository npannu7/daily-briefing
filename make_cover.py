#!/usr/bin/env python3
"""Draws the podcast cover (artwork.jpg, 1400x1400) from mathematics:
a loss landscape, and the path gradient descent actually takes across it."""
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import patheffects as pe
from matplotlib.colors import LinearSegmentedColormap

BG, INK, AMBER, MUTED = "#0A0F1E", "#F4F1EA", "#FFB547", "#8FA3C7"


def loss(x, y):
    """A landscape with one deep valley, a shallow decoy basin and a ridge."""
    return (0.08 * (x ** 2 + 0.6 * y ** 2)
            - 2.4 * np.exp(-((x - 1.15) ** 2 + (y + 0.85) ** 2) / 0.55)
            - 1.1 * np.exp(-((x + 1.3) ** 2 + (y - 1.1) ** 2) / 0.35)
            + 0.9 * np.exp(-((x + 0.1) ** 2 + (y - 0.1) ** 2) / 0.9))


def grad(p, h=1e-4):
    x, y = p
    return np.array([(loss(x + h, y) - loss(x - h, y)) / (2 * h),
                     (loss(x, y + h) - loss(x, y - h)) / (2 * h)])


def descent(start, lr=0.06, beta=0.6, steps=140):
    p, v, path = np.array(start, float), np.zeros(2), [np.array(start, float)]
    for _ in range(steps):
        v = beta * v - lr * grad(p)
        p = p + v
        path.append(p.copy())
    return np.array(path)


def main(out="artwork.jpg"):
    fig = plt.figure(figsize=(14, 14), dpi=100, facecolor=BG)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_facecolor(BG)
    ax.set_xlim(-3.2, 3.2)
    ax.set_ylim(-3.2, 3.2)
    ax.axis("off")

    xs = np.linspace(-3.4, 3.4, 600)
    X, Y = np.meshgrid(xs, xs)
    Z = loss(X, Y)
    cmap = LinearSegmentedColormap.from_list("wm", ["#5B6CFF", "#2EC4B6", "#1B2A4A"])
    ax.contour(X, Y, Z, levels=34, cmap=cmap, linewidths=1.6, alpha=0.75)
    ax.contour(X, Y, Z, levels=8, colors=[MUTED], linewidths=0.6, alpha=0.25)

    path = descent((-2.55, -2.15))
    ax.plot(path[:, 0], path[:, 1], color=AMBER, lw=3.2, alpha=0.95, solid_capstyle="round", zorder=4)
    moving = np.r_[True, np.linalg.norm(np.diff(path, axis=0), axis=1) > 0.04]
    pts = path[moving][::2]
    ax.scatter(pts[:, 0], pts[:, 1], s=70, color=AMBER, edgecolor=BG, linewidth=1.5, zorder=5)
    end = path[-1]
    for r, a in ((900, 0.10), (420, 0.22), (160, 1.0)):
        ax.scatter([end[0]], [end[1]], s=r, color=AMBER, alpha=a, zorder=6, linewidths=0)
    ax.scatter([path[0, 0]], [path[0, 1]], s=180, facecolor=BG, edgecolor=AMBER, linewidth=3, zorder=6)

    glow = [pe.withStroke(linewidth=14, foreground=BG)]
    fig.text(0.075, 0.885, "Weights", color=INK, fontsize=118, fontweight="bold",
             family="DejaVu Sans", va="center", path_effects=glow)
    fig.text(0.075, 0.775, "& Measures", color=INK, fontsize=118, fontweight="bold",
             family="DejaVu Sans", va="center", path_effects=glow)
    fig.text(0.08, 0.695, "AI, as it actually works.", color=AMBER, fontsize=44,
             family="DejaVu Sans", va="center", path_effects=glow)

    fig.text(0.92, 0.075, r"$\theta_{t+1} = \theta_t - \eta\,\nabla_\theta\,\mathcal{L}(\theta_t)$",
             color=MUTED, fontsize=40, ha="right", va="center", path_effects=glow)
    fig.text(0.08, 0.075, "DAILY  ·  TWO HOSTS", color=MUTED, fontsize=26, va="center",
             family="DejaVu Sans", path_effects=glow)

    fig.savefig(out, dpi=100, facecolor=BG, pil_kwargs={"quality": 90, "optimize": True})
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
