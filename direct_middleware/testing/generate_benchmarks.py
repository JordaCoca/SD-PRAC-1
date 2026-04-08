import random

def generate_unnumbered(n):
    """
            Genera un benchmark de tipo 'unnumbered'.
            n: número total de solicitudes (por ejemplo 2100)
    """
    filename = f"benchmark_unnumbered_{n}.txt"
    with open(filename, "w") as f:
        for i in range(n):
            f.write(f"BUY c{i} r{i}\n")
    print("Unnumbered benchmark guardado en", filename)


def generate_numbered(n):
    """
        Genera un benchmark de tipo 'numbered'.
        n: número total de solicitudes (por ejemplo 2100)
        El numero de silla es asignado aleatoriamente dentro del rango entre 1 y numero de peticiones
    """
    filename = f"benchmark_numbered_{n}.txt"
    with open(filename, "w") as f:
        for i in range(n):
            seat = random.randint(1, n)  # rango dinámico según tamaño
            f.write(f"BUY c{i} {seat} r{i}\n")
    print("Numbered benchmark guardado en", filename)

def generate_numbered_stress(n, total_seats=210):
    """
    Genera un benchmark de tipo 'numbered' para stress testing.
    Cada silla del rango 1..total_seats se repetirá de manera circular hasta completar n solicitudes.

    n: número total de solicitudes (por ejemplo 2100)
    total_seats: número de sillas disponibles (por defecto 210)
    """
    filename = f"benchmark_numbered_stress_{n}_{total_seats}.txt"
    with open(filename, "w") as f:
        for i in range(n):
            seat = (i % total_seats) + 1  # va de 1 a total_seats y se repite
            f.write(f"BUY c{i} {seat} r{i}\n")
    print("Benchmark de stress guardado en", filename)

if __name__ == "__main__":
    #generate_unnumbered(2100)
    generate_numbered(2100)
    generate_numbered_stress(2100)