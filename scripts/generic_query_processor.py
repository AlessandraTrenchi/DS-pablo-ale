from impl import RelationalQueryProcessor

class GenericQueryProcessor:
    def __init__(self, relational_query_processor, triple_store_query_processor):
        self.relational_query_processor = relational_query_processor
        self.triple_store_query_processor = triple_store_query_processor
        self.query_processors = []

    def addQueryProcessor(self, processor):
        self.query_processors.append(processor)
        return True

    def cleanQueryProcessor(self):
        self.query_processors = []
        return True

    def getPublicationsPublishedInYear(self, year):
        # Implement logic to retrieve publications published in the given year
        return self.relational_query_processor.getPublicationPublishedInYear(year)

    def getPublicationByAuthorId(self, author_id):
        # Implement logic to retrieve a publication by author ID
        return self.relational_query_processor.getPublicationsByAuthorId(author_id)

    def getMostCitedPublication(self):
        # Implement logic to retrieve the most cited publication
        return self.relational_query_processor.getMostCitedPublication()

    def getMostCitedVenue(self):
        # Implement logic to retrieve the most cited venue
        return self.relational_query_processor.getMostCitedVenue()

    def getVenuesByPublisherId(self, publisher_id):
        # Implement logic to retrieve venues by publisher ID
        return self.relational_query_processor.getVenuesByPublisherId(publisher_id)

    def getPublicationInVenue(self, venue_id):
        # Implement logic to retrieve publications in a specific venue
        return self.relational_query_processor.getPublicationInVenue(venue_id)

    def getJournalArticlesInIssue(self, issue, volume, journal_id):
        # Implement logic to retrieve journal articles in a specific issue
        return self.relational_query_processor.getJournalArticlesInIssue(issue, volume, journal_id)

    def getJournalArticlesInJournal(self, journal_id):
        # Implement logic to retrieve journal articles in a specific journal
        return self.relational_query_processor.getJournalArticlesInJournal(journal_id)

    def getProceedingsByEvent(self, event_partial_name):
        # Implement logic to retrieve proceedings by event partial name
        return self.relational_query_processor.getProceedingsByEvent(event_partial_name)

    def getPublicationAuthors(self, publication_id):
        # Implement logic to retrieve authors of a specific publication
        return self.relational_query_processor.getPublicationAuthors(publication_id)

    def getPublicationByAuthorName(self, author_partial_name):
        # Implement logic to retrieve publications by author name
        return self.relational_query_processor.getPublicationByAuthorName(author_partial_name)

    def getDistinctPublishersOfPublications(self, pub_id_list):
        # Implement logic to retrieve distinct publishers of publications
        return self.relational_query_processor.getDistinctPublishersOfPublications(pub_id_list)
