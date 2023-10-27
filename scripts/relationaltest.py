import unittest
from impl import Venue, Publication, IdentifiableEntity, Person, Organization

class TestVenueMethods(unittest.TestCase):
    def test_getTitle(self):
        venue = Venue(1, 2, "Sample Venue", "Conference")
        self.assertEqual(venue.getTitle(), "Sample Venue")

    def test_addPublisher(self):
        venue = Venue(1, 2, "Sample Venue", "Conference")
        organization = Organization(1, 2, "Sample Organization")
        venue.addPublisher(organization)
        self.assertIn(organization, venue.getPublishers())

class TestPublicationMethods(unittest.TestCase):
    def test_getPublicationYear(self):
        publication = Publication(1, "Sample Publication", "Journal Article", 2022, None, None, None, None, None, None, None)
        self.assertEqual(publication.getPublicationYear(), 2022)

    def test_getCitedPublications(self):
        publication = Publication(1, "Sample Publication", "Journal Article", 2022, None, None, None, None, None, None, None)
        cited_publication = Publication(2, "Cited Publication", "Journal Article", 2021, None, None, None, None, None, None, None)
        publication.addCitedPublication(cited_publication)
        self.assertIn(cited_publication, publication.getCitedPublications())

class TestIdentifiableEntityMethods(unittest.TestCase):
    def test_getIds(self):
        entity = IdentifiableEntity(1)
        self.assertEqual(entity.getIds(), [1])

class TestPersonMethods(unittest.TestCase):
    def test_getGivenName(self):
        person = Person(1, 2, "John", "Doe")
        self.assertEqual(person.getGivenName(), "John")

    def test_getFamilyName(self):
        person = Person(1, 2, "John", "Doe")
        self.assertEqual(person.getFamilyName(), "Doe")

class TestOrganizationMethods(unittest.TestCase):
    def test_getName(self):
        organization = Organization(1, 2, "Sample Organization")
        self.assertEqual(organization.getName(), "Sample Organization")

if __name__ == '__main__':
    unittest.main()
