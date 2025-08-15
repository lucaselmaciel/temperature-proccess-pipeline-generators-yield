from main.pipeline import pipeline


if __name__ == "__main__":
    with open("main/paths.txt") as file:
        paths = [line.strip() for line in file]

    for row in pipeline(paths):
        print(row)
