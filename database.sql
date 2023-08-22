CREATE DATABASE pabloale;
USE pabloale;
-- Higher level casses representing the entity-relationship model of the data

/* Entities from CSV data */
CREATE TABLE Publisher (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(50)
);

CREATE TABLE Event (
    id INT PRIMARY KEY AUTO_INCREMENT,
    event_detail VARCHAR(50)
);

CREATE TABLE Publication (
    id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    publication_year INT NOT NULL,
    issue INT,
    volume INT,
    chapter INT,
    publication_venue VARCHAR(255) NOT NULL,
    venue_type VARCHAR(50) NOT NULL,
    publisher_id VARCHAR(255),
    event_id INT,
    FOREIGN KEY (publisher_id) REFERENCES Publisher(id),
    FOREIGN KEY (event_id) REFERENCES Event(id)
);

/* Entities from JSON structure and reference UML */
CREATE TABLE IdentifiableEntity (
    id VARCHAR(50) PRIMARY KEY
);

CREATE TABLE Person (
    id INT PRIMARY KEY AUTO_INCREMENT,
    identifiableEntityId VARCHAR(50) REFERENCES IdentifiableEntity(id),
    givenName VARCHAR(50) NOT NULL,
    familyName VARCHAR(50) NOT NULL
);

CREATE TABLE Venue (
    id INT PRIMARY KEY AUTO_INCREMENT,
    IdentifiableEntityId VARCHAR(50) REFERENCES IdentifiableEntity(id),
    title VARCHAR(255),
    type VARCHAR(50) NOT NULL
);

CREATE TABLE Organization (
    id INT PRIMARY KEY AUTO_INCREMENT,
    IdentifiableEntityId VARCHAR(50) REFERENCES IdentifiableEntity(id),
    name VARCHAR(255) NOT NULL
);

CREATE TABLE PublicationVenue (
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id),
    VenueId INT REFERENCES Venue(id)
);

CREATE TABLE Citation (
    id INT PRIMARY KEY AUTO_INCREMENT,
    CitingPublicationId VARCHAR(255) REFERENCES Publication(id),
    CitedPublicationId VARCHAR(255) REFERENCES Publication(id)
);

CREATE TABLE BookChapter (
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id),
    chapterNumber INT NOT NULL
);

CREATE TABLE JournalArticle (
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id)
);

CREATE TABLE ProceedingsPaper (
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id)
);
