import random

def generate_unnumbered(filename="benchmark_unnumbered_20000.txt"):
    with open(filename, "w") as f:
        for i in range(30000):
            f.write(f"BUY c{i} r{i}\n")
    print("Unnumbered benchmark guardado en", filename)


def generate_numbered(filename="benchmark_numbered_60000.txt"):
    with open(filename, "w") as f:
        # 60000 purchases
        for i in range(60000):
            seat = random.randint(1, 20000)
            f.write(f"BUY c{i} {seat} r{i}\n")
    print("Numbered benchmark guardado en", filename)


if __name__ == "__main__":
    generate_unnumbered()
    generate_numbered()