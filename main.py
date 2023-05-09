import os


def main(show=False, output_path=None):
    try:
        names_ua = TaskOne(path='imdb_data/title.akas.tsv.gz', output_path=output_path)
        result1 = names_ua.get_data()
        if show:
            names_ua.show_table(result1)

        if output_path:
            names_ua.write_results()
    except:
        print('Res1 error')

    try:
        people_names_born19 = TaskTwo(path='imdb_data/name.basics.tsv.gz', output_path=output_path)
        result2 = people_names_born19.get_data()
        if show:
            people_names_born19.show_table(result2)

        if output_path:
            people_names_born19.write_results()
    except:
        print('Res2 error')

    try:
        runtime3 = TaskThree(path='imdb_data/title.basics.tsv.gz', output_path=output_path)
        result3 = runtime3.get_data()
        if show:
            runtime3.show_table(result3)

        if output_path:
            runtime3.write_results()
    except:
        print('Res3 error')


    try:
        df_temp = TaskFour(path='imdb_data/name.basics.tsv.gz', path1='imdb_data/title.principals.tsv.gz',
                           path2='imdb_data/title.akas.tsv.gz', output_path=output_path)
        result4 = df_temp.get_data()
        if show:
            df_temp.show_table(result4)

        if output_path:
            df_temp.write_results()
    except:
        print('Res4 error')

    try:
        df_temp = TaskFive(path='imdb_data/title.akas.tsv.gz', path1='imdb_data/title.basics.tsv.gz',
                           path2='imdb_data/title.ratings.tsv.gz', output_path=output_path)
        result5 = df_temp.get_data()
        if show:
            df_temp.show_table(result5)

        if output_path:
            df_temp.write_results()
    except:
        print('Res5 error')

    try:
        df_temp = TaskSix(path='imdb_data/title.basics.tsv.gz',
                          films_episode_df_path='imdb_data/title.episode.tsv.gz', output_path=output_path)

        result6 = df_temp.get_data()
        if show:
            df_temp.show_table(result6)
        if output_path:
            df_temp.write_results()
    except:
        print('Res6 error')

    try:
        df_temp = TaskSeven(path='imdb_data/title.basics.tsv.gz',
                            films_rating_path='imdb_data/title.ratings.tsv.gz', output_path=output_path)

        result7 = df_temp.get_data()
        if show:
            df_temp.show_table(result7)
        if output_path:
            df_temp.write_results()
    except:
        print('Res7 error')

    try:
        df_temp = TaskEight(path='imdb_data/title.basics.tsv.gz',
                            films_rating_path='imdb_data/title.ratings.tsv.gz', output_path=output_path)

        result8 = df_temp.get_data()
        if show:
            df_temp.show_table(result8)
        if output_path:
            df_temp.write_results()
    except:
        print('Res8 error')


if __name__ == "__main__":
    output_dir = os.path.dirname(os.path.abspath(__file__)) + "\\results"
    main(show=True, output_path=output_dir)



