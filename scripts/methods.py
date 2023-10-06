# Defining data model classes based on SQL schema and their relationships 


#__init__ is a method called constructor called whenever you create an instance/object of a class
# self refers to the instance being created
#self is used for instance reference -> When you create an object from a class, Python automatically passes the instance itself as the first parameter to the __init__ method. This allows you to access and manipulate the attributes of that instance. ATTRIBITE INITIALIZATION -> Inside the __init__ method, you can set initial values for the attributes of the instance. By using the self parameter, you're associating those values with the specific instance being created. METHOD ACCESS -> Once the instance is created, you can access its attributes and call its methods using the self reference within other methods of the class.
class Venue:
    def __init__(self, id, identifiable_entity_id, title, type):
        self.id = id
        self.identifiable_entity_id = identifiable_entity_id
        self.title = title
        self.type = type
class Publisher:
    def __init__(self, id, name): #init is a constructor that initializes the attributes when an instance of the class is created
        self.id = id #init has 2 attributes: id and name
        self.name = name
# each class has attributes corresponding to the columns in the table, basic structure
class Event:
    def __init__(self, id, event_detail):
        self.id = id
        self.event_detail = event_detail

class Publication:
    def __init__(self, id, title, type, publication_year, issue, volume,
                 chapter, publication_venue, venue_type, publisher_id, event_id):
        self.id = id
        self.title = title
        self.type = type
        self.publication_year = publication_year
        self.issue = issue
        self.volume = volume
        self.chapter = chapter
        self.publication_venue = publication_venue
        self.venue_type = venue_type
        self.publisher_id = publisher_id
        self.event_id = event_id
        self.cited_publications = [] #initialize an empty string for cited publications
        self.authors = set() #initialize an empty set for authors

    def getPublicationYear(self) -> int or None:
        if isinstance(self.publication_year, int):
            return self.publication_year
        else:
            return None

    def getTitle(self) -> str:
        return self.title

    def getCitedPublications(self) -> list['Publication']:
        # return the list of cited publications for this publication
        return self.cited_publications  

    def getPublicationVenue(self) -> Venue:
        """
        Retrieve the publication venue associated with this publication.

        Returns:
            Venue: The Venue object representing the publication venue.
        """
        return self.publication_venue

    def getAuthors(self) -> set['Person']:
        #return the set of authors associated with this publication
        return self.authors  

class IdentifiableEntity:
    def __init__(self, id):
        self.id = id

class Person:
    def __init__(self, id, identifiable_entity_id, given_name, family_name):
        self.id = id
        self.identifiable_entity_id = identifiable_entity_id
        self.given_name = given_name
        self.family_name = family_name
    
    def getGivenName (self):
        return self.given_name
    
    def getFamilyName(self):
        return self.family_name

class Organization:
    def __init__(self, id, identifiable_entity_id, name):
        self.id = id
        self.identifiable_entity_id = identifiable_entity_id
        self.name = name

class PublicationVenue:
    def __init__(self, id, publication_id, venue_id):
        self.id = id
        self.publication_id = publication_id
        self.venue_id = venue_id

class Citation:
    def __init__(self, id, citing_publication_id, cited_publication_id):
        self.id = id
        self.citing_publication_id = citing_publication_id
        self.cited_publication_id = cited_publication_id

class BookChapter:
    def __init__(self, id, publication_id, chapter_number):
        self.id = id
        self.publication_id = publication_id
        self.chapter_number = chapter_number

class JournalArticle:
    def __init__(self, id, publication_id):
        self.id = id
        self.publication_id = publication_id

class ProceedingsPaper:
    def __init__(self, id, publication_id):
        self.id = id
        self.publication_id = publication_id
