import matplotlib.pyplot as plt
import matplotlib.animation as animation
import numpy as np

npes = 4
window = 60.

# Initialize figure and axis
fig, ax = plt.subplots()
xs, ys = [], []

def animate(i):
    ax.clear()
    for i in range(npes):
    # Read data from file
        try:
            data = np.loadtxt('memory_log_%i.txt' % i, delimiter=',')
            xs = data[:, 0]  # X values
            ys = data[:, 1]  # Y values

            xmax = xs[-1]
            xmin = xmax - window

            indices = np.where(xs > xmin)

            xs = xs[indices]
            ys = ys[indices]

            # Clear previous plot and redraw
            ax.plot(xs, ys, label='PE %i' % i)
        except Exception as e:
            print(f"Error reading file: {e}")
    ax.set_title('Real-Time Data Plot')
    ax.set_xlabel('Time')
    ax.set_ylabel('Memory usage')
    ax.legend()

# Create animation that updates every 1000 ms (1 second)
ani = animation.FuncAnimation(fig, animate, interval=1000)

plt.tight_layout()
plt.show()

