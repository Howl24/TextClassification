from cassandra.cluster import Cluster
from cassandra.cluster import NoHostAvailable
from cassandra import InvalidRequest
from cassandra.query import BoundStatement
import csv

SUCCESSFUL_OPERATION = True
UNSUCCESSFUL_OPERATION = False

OFFER_ID_FIELD = "Id"
OFFER_YEAR_FIELD = "Año"
OFFER_MONTH_FIELD = "Mes"
OFFER_SOURCE_FIELD = "Fuente"


class Offer:
    session = None
    keyspace = ""
    offers_table = "new_offers"
    select_stmt = None
    select_all_stmt = None
    insert_stmt = None

    def __init__(self, year=0, month=0, id="",
                 features={}, careers=set(), skills={},
                 source=""):
        self.year = year
        self.month = month
        self.id = id
        self.features = features

        if careers is None:
            careers = set()
        self.careers = careers

        self.skills = skills
        self.source = source
    
    @classmethod
    def CreateTables(cls):
        cmd1 = """
               CREATE TABLE IF NOT EXISTS {0} (
               id text,
               year int,
               month int,
               features map<text,text>,
               careers set<text>,
               PRIMARY KEY ((id, year, month)));
               """.format(cls.offers_table)

        try:
            cls.session.execute(cmd1)
        except:
            return constant.FAIL

        return constant.DONE

    @classmethod
    def ConnectToDatabase(cls, cluster=None):
        if not cluster:
            cluster = Cluster()

        try:
            cls.session = cluster.connect(cls.keyspace)
        except NoHostAvailable as e:
            print("Ningun servicio de cassandra esta disponible.")
            print("Inicie un servicio con el comando " +
                  "\"sudo cassandra -R \"")
            print()
            return UNSUCCESSFUL_OPERATION

        return SUCCESSFUL_OPERATION

    @classmethod
    def SetKeyspace(cls, keyspace):
        cls.keyspace = keyspace
        try:
            cls.session.set_keyspace(cls.keyspace)
        except InvalidRequest:
            print("El keyspace no existe")
            print()
            return UNSUCCESSFUL_OPERATION

        cls.PrepareStatements()
        return SUCCESSFUL_OPERATION

    @classmethod
    def PrepareStatements(cls, keyspace=None):
        if keyspace:
            if cls.SetKeyspace(keyspace) == UNSUCCESSFUL_OPERATION:
                return UNSUCCESSFUL_OPERATION

        cmd_select = """
                     SELECT * FROM {0} WHERE
                     year = ? AND
                     month = ? AND
                     id = ?;
                     """.format(cls.offers_table)

        cmd_select_all = """
                         SELECT * FROM {0}
                         """.format(cls.offers_table)

        cmd_insert = """
                     INSERT INTO {0}
                     (id, year, month, careers, features)
                     VALUES
                     (?, ?, ?, ?, ?);
                     """.format(cls.offers_table)

        try:
            cls.select_stmt = cls.session.prepare(cmd_select)
            prepared_stmt = cls.session.prepare(cmd_select_all)
            cls.select_all_stmt = BoundStatement(prepared_stmt, fetch_size=10)
            cls.insert_stmt = cls.session.prepare(cmd_insert)
        except InvalidRequest:
            print("Tabla no configurada")
            raise
            return UNSUCCESSFUL_OPERATION

        return SUCCESSFUL_OPERATION

    def insert(self):
        if Offer.keyspace != self.source:
            Offer.SetKeyspace(self.source)

        self.session.execute(self.insert_stmt, 
                             (self.id,
                              self.year,
                              self.month,
                              self.careers,
                              self.features,
                              ))

    @classmethod
    def Select(cls, year, month, id, source):
        if source != cls.keyspace:
            cls.SetKeyspace(source)

        rows = cls.session.execute(cls.select_stmt,
                                   (year, month, id))

        if not rows:
            return None
        else:
            return Offer.ByCassandraRow(rows[0], source)

    @classmethod
    def ByCassandraRow(cls, row, source):
        return cls(row.year,
                   row.month,
                   row.id,
                   row.features,
                   row.careers,
                   source=source)

    @classmethod
    def SelectAll(cls, source):
        cls.SetKeyspace(source)
        rows = cls.session.execute(cls.select_all_stmt)

        if not rows:
            return None
        else:
            return cls.ByCassandraRows(rows, source)

    @classmethod
    def SelectSince(cls, source, date):
        year = date[0]
        month = date[1]
        cls.SetKeyspace(source)
        rows = cls.session.execute(cls.select_all_stmt)

        if not rows:
            return None
        else:
            selected_rows = []
            for row in rows:
                if (row.year > year) or \
                   ((row.year == year) and (row.month >= month)):
                    selected_rows.append(row)

            return cls.ByCassandraRows(selected_rows, source)

    @classmethod
    def ByDateRange(cls, min_date, max_date, source):
        cls.SetKeyspace(source)
        rows = cls.session.execute(cls.select_all_stmt)

        if not rows:
            return None

        else:
            selected_rows = []
            for row in rows:
                offer_month = row.month
                offer_year = row.year
                offer_date = (offer_month, offer_year)
                if cls._check_date_range(offer_date, min_date, max_date):
                    selected_rows.append(row)

            return cls.ByCassandraRows(selected_rows, source)

    @staticmethod
    def _check_date_range(offer_date, min_date, max_date):
        # TODO
        # Replace date by a class

        MONTH = 0
        YEAR = 1
        MONTHS_PER_YEAR = 12

        offer_months = offer_date[MONTH] + offer_date[YEAR] * MONTHS_PER_YEAR
        min_date_months = min_date[MONTH] + min_date[YEAR] * MONTHS_PER_YEAR
        max_date_months = max_date[MONTH] + max_date[YEAR] * MONTHS_PER_YEAR

        if offer_months >= min_date_months and offer_months <= max_date_months:
            return True
        else:
            return False

    @classmethod
    def ByCassandraRows(cls, rows, source):
        offers = []
        for row in rows:
            offer = cls(row.year, row.month, row.id, row.features, row.careers,
                        source=source)
            offers.append(offer)

        return offers

    def get_text(self, feature_list, delimiter=" "):
        """
        Get offer text from feature list.
        concatenated by delimiter
        """

        text = ""
        for feature in feature_list:
            if feature in self.features:
                text += self.features[feature] + delimiter

        return text

    @classmethod
    def FromConfiguration(cls, configuration):
        # TODO 
        # Include filters in configuration

        # Career and date range filter 
        filter_career = "ECONOMÍA"
        filter_min_date = (1, 2013)
        filter_max_date = (12, 2017)

        # All offers - No filter purpose
        #offers = Offer.SelectAll(configuration.source)

        # Filter offers by date range
        offers = Offer.ByDateRange(filter_min_date,
                                   filter_max_date,
                                   configuration.source)

        print("ofertas: ", len(offers))
        filtered_offers = []
        for offer in offers:
            # Filter offers by career
            offer_careers = [career.strip() for career in offer.features['Majors/Concentrations'].split(",")]
            offer_month = offer.month
            offer_year = offer.year

            if filter_career in offer_careers:
                filtered_offers.append(offer)

        offers = filtered_offers
        return offers

    @classmethod
    def PrintAsCsv(cls, offers,
                        filename,
                        configuration=None,
                        print_id=False,
                        print_labels=False,
                        field=None,
                        labels=None):

        conf_features = ["Job Title", "Description", "Qualifications"]
        
        with open(filename, "w") as csvfile:
            fieldnames = []

            if print_id is True:
                fieldnames.append(OFFER_ID_FIELD)
                fieldnames.append(OFFER_YEAR_FIELD)
                fieldnames.append(OFFER_MONTH_FIELD)
                fieldnames.append(OFFER_SOURCE_FIELD)

            fieldnames += list(conf_features)

            if print_labels:
                fieldnames += list(labels)

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for offer in offers:
                write_dict = {}
                if print_id is True:
                    write_dict[OFFER_ID_FIELD] = offer.id
                    write_dict[OFFER_YEAR_FIELD] = offer.year
                    write_dict[OFFER_MONTH_FIELD] = offer.month
                    write_dict[OFFER_SOURCE_FIELD] = offer.source

                for feature_name in conf_features:
                    if feature_name in offer.features:
                        write_dict[feature_name] = offer.features[feature_name]

                if print_labels:
                    if field in offer.features:
                        offer_labels = offer.features[field].split(",")
                        for label in labels:
                            if label in offer_labels:
                                write_dict[label] = "X"
                            else:
                                write_dict[label] = ""
                    else:
                        for label in labels:
                            write_dict[label] = ""

                writer.writerow(write_dict)
