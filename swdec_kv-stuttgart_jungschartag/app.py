# Customized Pretix2Nextcloud instance for kv-stuttgart-jungschartag
# Needs P2N.py to function

import pandas as pd
import logging
import sys
import os

# Add current directory
sys.path.append(os.path.dirname(__file__))
# Add parent directory
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# import P2N.py
try:
    from P2N import Main, Environment, PretixAPI
except ImportError:
    raise ImportError("Could not import P2N.py from current or parent directory.")


Environment().set_defaults(
    default_pretix_url="https://tickets.swdec.de",
    default_pretix_organizer_slug="kv-stuttgart",
    default_excel_max_column_width=30,
    default_nextcloud_url="https://jcloud.swdec.de",
    default_nextcloud_upload_dir="Anmeldungen_Jungschartag",
    default_timezone="Europe/Berlin",
    default_interval_minutes=15,
    default_check_interval_seconds=60,
    default_run_once=False,
    default_logging_level="INFO",
)


class Dataframe:
    def __init__(self, success_on_last_run: bool = False):
        """
        Initialize, fetch data from Pretix API and load into different desired dataframes.
        """
        pretix = PretixAPI()
        env = Environment()

        self.time_zone = env.get_timezone()

        self.raw_df = pretix.get_raw_df()
        # check for new fetched data and raise exception if no new data occured so that Main can skip this run
        pretix.check_for_new_fetched_data(self.raw_df, success_on_last_run)
        
        self.towns_list = pretix.get_answer_choices_from_question("Ich melde mich über folgende Ortschaft an")

        self.sorted_df = self._get_sorted_df()
        self.town_dfs = self._get_town_dfs()
        self.numbers_overview = self._get_numbers_df()
        self.diet_numbers_df = self._get_diet_numbers_df()
        self.attendees_with_intolerances = self._get_attendees_with_intolerances()


    def _get_sorted_df(self) -> pd.DataFrame:
        """
        Process raw dataframe to create a sorted dataframe with required columns.
        """

        df = self.raw_df.copy()

        # removed all cancelled registrations
        df = df[df["status"] != "c"]

        # rename needed columns
        renames = {
            "order_code": "Bestellnummer",
            "email": "E-Mail",
            "date": "Anmeldedatum",
            "item_name": "Art",
            "attendee_firstname": "Vorname",
            "attendee_lastname": "Nachname",
            "Essensunverträglichkeiten": "Essensunverträglichkeiten Ja/Nein",
            "Welche Unverträglichkeiten?": "Essensunverträglichkeiten",
            "Ich melde mich über folgende Ortschaft an": "Ortschaft",
            "Ich biete eine Fahrgemeinschaft an": "Fahrer Angebot Eltern",
            "Ich stelle mich als Fahrer bereit": "Fahrer Angebot Mitarbeiter",
            "Ich bin Ortsverantwortlicher.": "Ortsverantwortlicher",
            "Telefonnummer": "Telefonnummer Mitarbeiter",
        }
        df = df.rename(columns=renames)
        
        # combine "Fahrer Angebot Eltern" and "Fahrer Angebot Mitarbeiter" to one column "Telefonnummer"
        df["Fahrer Angebot"] = df["Fahrer Angebot Eltern"].combine_first(df["Fahrer Angebot Mitarbeiter"])

        # combine "Telefonnummer der Eltern" and "Telefonnummer Mitarbeiter" to one column "Telefonnummer"
        df["Telefonnummer"] = df["Telefonnummer der Eltern"].combine_first(
            df["Telefonnummer Mitarbeiter"]
        )

        # filter for columns and set their order
        wanted_columns = [
            "Ortschaft",
            "Art",
            "Nachname",
            "Vorname",
            "Telefonnummer",
            "E-Mail",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Sonstiges",
            "Ortsverantwortlicher",
            "Fahrer Angebot",
            "Anmeldedatum",
            "Bestellnummer",
        ]
        df = df.filter(wanted_columns)

        # change date format
        df["Anmeldedatum"] = (
            pd.to_datetime(df["Anmeldedatum"], utc=True)
            .dt.tz_convert(self.time_zone)
            .dt.strftime("%Y-%m-%d %H:%M")
        )

        # sort (by "Ortschaft", then by "Art", then by "Nachname" and then by "Vorname") and reset index numbers
        df = df.sort_values(
            by=["Ortschaft", "Art", "Nachname", "Vorname"], ascending=True
        )
        df.index = range(1, len(df) + 1)

        logging.info("Sorted raw data.")

        return df

    def _get_town_dfs(self) -> dict[str, pd.DataFrame]:
        """
        Process sorted dataframe to create a dictionary of dataframes filtered by town.
        """

        df = self.sorted_df.copy()

        # filter for columns and set their order
        wanted_columns = [
            "Ortschaft",
            "Art",
            "Nachname",
            "Vorname",
            "Telefonnummer",
            "E-Mail",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Sonstiges",
            "Fahrer Angebot",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)

        # sort by town:
        df_by_town_dict = {}
        df_towns = (df["Ortschaft"].dropna().astype(str).str.strip().unique())
        towns = sorted(set(self.towns_list) | set(df_towns))
        for town in towns:
            # filter by town, drop column "Ortschaft" and reset index numbers
            town_df = df[df["Ortschaft"] == town]
            town_df = town_df.drop(columns=["Ortschaft"])
            town_df.index = range(1, len(town_df) + 1)

            df_by_town_dict[town] = town_df

        logging.info("Filtered sorted data by town.")

        return df_by_town_dict

    def _get_numbers_df(self) -> pd.DataFrame:
        """
        Calculate and return a dataframe with counts of "Jungscharler", "Mitarbeiter", and total by town.
        """

        numbers_df = pd.DataFrame(
            {"Ortschaft": [], "Jungscharler": [], "Mitarbeiter": [], "Gesamt": []}
        )

        # make Ortschaft the index
        numbers_df = numbers_df.set_index("Ortschaft")

        df = self.sorted_df

        number_of_kids = len(df[df["Art"].str.contains("Jungscharler", na=False)])
        number_of_staff = len(df[df["Art"].str.contains("Mitarbeiter", na=False)])
        number_total = len(df)

        # add row to numbers_df
        numbers_df.loc["GESAMT"] = [number_of_kids, number_of_staff, number_total]

        # filter by town:
        for town in self.towns_list:
            town_df = df[df["Ortschaft"] == town]

            town_kids = len(
                town_df[town_df["Art"].str.contains("Jungscharler", na=False)]
            )
            town_staff = len(
                town_df[town_df["Art"].str.contains("Mitarbeiter", na=False)]
            )
            town_total = len(town_df)

            # add row to numbers_df
            numbers_df.loc[town] = [town_kids, town_staff, town_total]
            
        logging.info("Cathegorized sorted data into numbers.")

        return numbers_df
    
    def _get_diet_numbers_df(self) -> pd.DataFrame:
        """
        Calculate and return a dataframe with counts of different dietary restrictions.
        """

        numbers_df = pd.DataFrame(
            {"Ernährung": [], "Jungscharler": [], "Mitarbeiter": [], "Gesamt": []}
        )

        # make Ernährung the index
        numbers_df = numbers_df.set_index("Ernährung")
        
        for i in ["Keine Besonderheiten", "Kein Schweinefleisch", "Vegetarisch", "Gesamt"]:

            df = self.sorted_df.copy()
            
            # filter for current group
            if i != "Gesamt":
                df = df[df["Ernährung"].str.contains(i, na=False)]

            number_of_kids = len(df[df["Art"].str.contains("Jungscharler", na=False)])
            number_of_staff = len(df[df["Art"].str.contains("Mitarbeiter", na=False)])
            number_total = len(df)

            # add row to numbers_df
            numbers_df.loc[i] = [number_of_kids, number_of_staff, number_total]
            
        logging.info("Cathegorized sorted data into dietary numbers.")

        return numbers_df
    
    def _get_attendees_with_intolerances(self) -> pd.DataFrame:
        """
        Returns df with only the attendees with dietary intolerances.
        """
        df = self.sorted_df.copy()
        
        # sort for "Essensunverträglichkeiten" that are not empty
        df = df[df["Essensunverträglichkeiten"].notna()]

        # filter for columns and set their order
        wanted_columns = [
            "Nachname",
            "Vorname",
            "Ortschaft",
            "Art",
            "Telefonnummer",
            "E-Mail",
            "Ernährung",
            "Essensunverträglichkeiten",
            "Sonstiges",
            "Anmeldedatum",
        ]
        df = df.filter(wanted_columns)
        
        # sort by "Nachname", then by "Vorname" and then by "Ortschaft" and reset index numbers
        df = df.sort_values(
            by=["Nachname", "Vorname", "Ortschaft"], ascending=True
        )
        df.index = range(1, len(df) + 1)
        
        logging.info("Filtered sorted data by intolerances.")
        
        return df


class CustomMain(Main):
    def main(self):
        """
        Main function to generate Excel files and upload them to Nextcloud.
        """
        
        # fetch and sort data
        dataframe = Dataframe(self.success_on_last_run)

        # generate and upload excel file for raw data
        self.upload(dataframe.raw_df, "Raw_Data", filterable=True)

        # generate and upload excel file for all attendees
        self.upload(dataframe.sorted_df, "Alle", filterable=True)

        # generate and upload excel file for town-wise attendees
        for town in dataframe.town_dfs:
            df = dataframe.town_dfs[town]
            self.upload(df, town, subdir="Nach_Orten")

        # generate and upload excel file for numbers overview
        self.upload(dataframe.numbers_overview, "Anmeldezahlen")
        
        # generate and upload excel file for diet numbers overview
        self.upload(dataframe.diet_numbers_df, "Küche_Gesamtzahlen")

        # generate and upload excel file for attendees with intolerances
        self.upload(dataframe.attendees_with_intolerances, "Küche_Unverträglichkeiten")

        self.cloud.upload_last_updated()
        
        self.cloud.upload_docker_image_version()


if __name__ == "__main__":
    CustomMain().run()
