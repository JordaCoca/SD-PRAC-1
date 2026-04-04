import random

def generate_unnumbered(n):
    filename = f"benchmark_unnumbered_{n}.txt"
    with open(filename, "w") as f:
        for i in range(n):
            f.write(f"BUY c{i} r{i}\n")
    print("Unnumbered benchmark guardado en", filename)


def generate_numbered(n):
    filename = f"benchmark_numbered_{n}.txt"
    with open(filename, "w") as f:
        for i in range(n):
            seat = random.randint(1, n)  # rango dinámico según tamaño
            f.write(f"BUY c{i} {seat} r{i}\n")
    print("Numbered benchmark guardado en", filename)

if __name__ == "__main__":
    generate_unnumbered(1000)
    generate_numbered(1000)