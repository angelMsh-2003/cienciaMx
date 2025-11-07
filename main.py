import argparse
from src.stats.stats import Stats

def main():
    parser = argparse.ArgumentParser(
        description="CLI para obtener estadísticas de un dataset JSON."
    )
    parser.add_argument(
        "--file", "-f",
        type=str,
        required=True,
        help="Path to the folder containing the JSON files to be analyzed"
    )
    parser.add_argument(
        "--oldest", action="store_true",
        help="Muestra el ítem más antiguo"
    )
    parser.add_argument(
        "--newest", action="store_true",
        help="Muestra el ítem más reciente"
    )
    parser.add_argument(
        "--language", action="store_true",
        help="Muestra el conteo de idiomas"
    )
    parser.add_argument (
        "--number_of_items", action="store_true",
        help="Show number of items"
    )

    args = parser.parse_args()

    st = Stats (args.file)

    data = st.load_data()

    if args.oldest:
        print(st.get_oldest_item(data))

    if args.newest:
        print(st.get_newest_item(data))

    if args.language:
        print(st.get_item_language(data))


    else:
        parser.print_help()


if __name__ == "__main__":
    main()
