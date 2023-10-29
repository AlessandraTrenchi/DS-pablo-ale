# Import the necessary classes
from impl import RelationalQueryProcessor, TriplestoreQueryProcessor, RelationalDataProcessor, TriplestoreDataProcessor

# Create and configure the relational database
rel_path = "pabloale.db"
rel_dp = RelationalDataProcessor()
try:
    rel_dp.setDbPath(rel_path)
    rel_dp.uploadData("data/relational_publications.csv")
    rel_dp.uploadData("data/relational_other_data.json")
except Exception as e:
    print(f"Error while configuring the relational database: {str(e)}")
    # You can choose to exit or handle the error as needed

# Create and configure the RDF triplestore (ensure your Blazegraph instance is running)
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
try:
    grp_dp.setEndpointUrl(grp_endpoint)
    grp_dp.uploadData("data/graph_publications.csv")
    grp_dp.uploadData("data/graph_other_data.json")
except Exception as e:
    print(f"Error while configuring the RDF triplestore: {str(e)}")
    # You can choose to exit or handle the error as needed

# Create query processors for both databases
try:
    rel_qp = RelationalQueryProcessor(rel_path)
    grp_qp = TriplestoreQueryProcessor(grp_endpoint)
except Exception as e:
    print(f"Error while creating query processors: {str(e)}")
    # You can choose to exit or handle the error as needed

class GenericQueryProcessor:
    def __init__(self, relational_query_processor, triple_store_query_processor):
        self.relational_query_processor = relational_query_processor
        self.triple_store_query_processor = triple_store_query_processor
        self.query_processors = []

    def addQueryProcessor(self, processor):
        try:
            self.query_processors.append(processor)
            return True
        except Exception as e:
            print(f"Error while adding query processor: {str(e)}")
            return False

    def cleanQueryProcessor(self):
        try:
            self.query_processors = []
            return True
        except Exception as e:
            print(f"Error while cleaning query processors: {str(e)}")
            return False

    def getPublicationsPublishedInYear(self, year):
        try:
            # Implement logic to retrieve publications published in the given year
            return self.relational_query_processor.getPublicationPublishedInYear(year)
        except Exception as e:
            print(f"Error in getPublicationsPublishedInYear: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getPublicationByAuthorId(self, author_id):
        try:
            # Implement logic to retrieve a publication by author ID
            return self.relational_query_processor.getPublicationsByAuthorId(author_id)
        except Exception as e:
            print(f"Error in getPublicationByAuthorId: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getMostCitedPublication(self):
        try:
            # Implement logic to retrieve the most cited publication
            return self.relational_query_processor.getMostCitedPublication()
        except Exception as e:
            print(f"Error in getMostCitedPublication: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getMostCitedVenue(self):
        try:
            # Implement logic to retrieve the most cited venue
            return self.relational_query_processor.getMostCitedVenue()
        except Exception as e:
            print(f"Error in getMostCitedVenue: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getVenuesByPublisherId(self, publisher_id):
        try:
            # Implement logic to retrieve venues by publisher ID
            return self.relational_query_processor.getVenuesByPublisherId(publisher_id)
        except Exception as e:
            print(f"Error in getVenuesByPublisherId: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getPublicationInVenue(self, venue_id):
        try:
            # Implement logic to retrieve publications in a specific venue
            return self.relational_query_processor.getPublicationInVenue(venue_id)
        except Exception as e:
            print(f"Error in getPublicationInVenue: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getJournalArticlesInIssue(self, issue, volume, journal_id):
        try:
            # Implement logic to retrieve journal articles in a specific issue
            return self.relational_query_processor.getJournalArticlesInIssue(issue, volume, journal_id)
        except Exception as e:
            print(f"Error in getJournalArticlesInIssue: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getJournalArticlesInJournal(self, journal_id):
        try:
            # Implement logic to retrieve journal articles in a specific journal
            return self.relational_query_processor.getJournalArticlesInJournal(journal_id)
        except Exception as e:
            print(f"Error in getJournalArticlesInJournal: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getProceedingsByEvent(self, event_partial_name):
        try:
            # Implement logic to retrieve proceedings by event partial name
            return self.relational_query_processor.getProceedingsByEvent(event_partial_name)
        except Exception as e:
            print(f"Error in getProceedingsByEvent: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getPublicationAuthors(self, publication_id):
        try:
            # Implement logic to retrieve authors of a specific publication
            return self.relational_query_processor.getPublicationAuthors(publication_id)
        except Exception as e:
            print(f"Error in getPublicationAuthors: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getPublicationByAuthorName(self, author_partial_name):
        try:
            # Implement logic to retrieve publications by author name
            return self.relational_query_processor.getPublicationByAuthorName(author_partial_name)
        except Exception as e:
            print(f"Error in getPublicationByAuthorName: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

    def getDistinctPublishersOfPublications(self, pub_id_list):
        try:
            # Implement logic to retrieve distinct publishers of publications
            return self.relational_query_processor.getDistinctPublishersOfPublications(pub_id_list)
        except Exception as e:
            print(f"Error in getDistinctPublishersOfPublications: {str(e)}")
            # You can choose to return an error value, log the error, or handle it as needed.

# Now define and call your query methods with try-except blocks to handle any exceptions.
