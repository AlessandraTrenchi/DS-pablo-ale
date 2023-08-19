/*mettere not null ovunque/

CREATE DATABASE mydatabase; /*creating the database*/()
USE mydatabase;

/*data from JSON contains info about publishers, references, venues, and authors*/

CREATE TABLE IdentifiableEntity (
    id VARCHAR(255) PRIMARY KEY
);
/*Person entity has a relationship with an Identifiable Entity*/
/*The identifiableEntityId column in both the "Organization" and "Publisher" tables references the id column in the "IdentifiableEntity" table*/
CREATE TABLE Person (
    id INT PRIMARY KEY AUTO_INCREMENT,
    identifiableEntityId VARCHAR(255), /* column that references the id column in the IdentifiableEntity table */
    givenName VARCHAR(255) NOT NULL,
    familyName VARCHAR(255) NOT NULL,
    FOREIGN KEY (identifiableEntityId) REFERENCES IdentifiableEntity (id)
);
CREATE TABLE Publication(
    id INT PRIMARY KEY AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL,
    publicationYear INT NOT NULL,
    issue INT, /*i select issue as part of the broader element*/
    volume INT, /* volume is moved from JournalArticle to the broader element to normalize the data*/
    citesPublicationId INT,
    chapter INT NOT NULL, /*moved from BookChapter*/
    FOREIGN KEY (citesPublicationId) REFERENCES Publication(id) /*self-referential relationship where a publication can reference another publication that it cites*/
);
CREATE TABLE PublicationAuthor( /*many-to-many relationship*/
    publicationId INT, /*These columns will hold the foreign key references to the "Publication" and "Person" entitie*/
    authorId INT,
    PRIMARY KEY (publicationId, authorId),
    FOREIGN KEY (publicationId) REFERENCES Publication(id),
    FOREIGN KEY (authorId) REFERENCES Person(id)
);
CREATE TABLE JournalArticle (
    id INT PRIMARY KEY AUTO_INCREMENT,
    publicationId INT,
    FOREIGN KEY (publicationId) REFERENCES Publication (id)
);
CREATE TABLE BookChapter (
    id INT PRIMARY KEY AUTO_INCREMENT,
    publicationId INT,
    FOREIGN KEY (publicationId) REFERENCES Publication (id)
);

CREATE TABLE ProceedingsPaper (
    id INT PRIMARY KEY AUTO_INCREMENT,
    publicationId INT,
    FOREIGN KEY (publicationId) REFERENCES Publication (id)
);
CREATE TABLE Organization (
    id INT PRIMARY KEY AUTO_INCREMENT,
    publisherId VARCHAR(255),
    name VARCHAR(255) NOT NULL, 
    FOREIGN KEY (publisherId) REFERENCES IdentifiableEntity (id)
);
CREATE TABLE PublisherEntity (
    organizationId INT,
    identifiableEntityId INT,
    PRIMARY KEY (organizationId, identifiableEntityId),
    FOREIGN KEY (organizationId) REFERENCES Organization(id),
    FOREIGN KEY (identifiableEntityId) REFERENCES IdentifiableEntity(id)
);
CREATE TABLE Venue (
    id INT PRIMARY KEY AUTO_INCREMENT,
    identifiableEntityId VARCHAR(255),
    organizationId VARCHAR(255),
    title VARCHAR(255),
    FOREIGN KEY (identifiableEntityId) REFERENCES IdentifiableEntity (id),
    FOREIGN KEY (organizationId) REFERENCES Organization (id)
);
CREATE TABLE PublicationVenue (
    publicationId INT,
    venueId INT,
    PRIMARY KEY (publicationId, venueId),
    FOREIGN KEY (publicationId) REFERENCES Publication(id),
    FOREIGN KEY (venueId) REFERENCES Venue(id)
);
CREATE TABLE Proceedings (
    id INT PRIMARY KEY AUTO_INCREMENT,
    venueId INT,
    event VARCHAR(255) NOT NULL, 
    FOREIGN KEY (venueId) REFERENCES Venue (id)
);

CREATE TABLE Book (
    id INT PRIMARY KEY AUTO_INCREMENT,
    venueId INT,
    FOREIGN KEY (venueId) REFERENCES Venue (id)
);

CREATE TABLE Journal (
    id VARCHAR(255) PRIMARY KEY,
    venueId INT,
    FOREIGN KEY (venueId) REFERENCES Venue (id)
);

CREATE TABLE venue_type (
    id INT PRIMARY KEY AUTO_INCREMENT,
    bookId INT,
    journalId VARCHAR(255),
    proceedingsId INT,
    FOREIGN KEY (bookId) REFERENCES Book(id),
    FOREIGN KEY (journalId) REFERENCES Journal(id),
    FOREIGN KEY (proceedingsId) REFERENCES Proceedings(id)
);

LOAD DATA INFILE './relational_publications.csv'
INTO TABLE Publication /* Target table for csv file*/
FIELDS TERMINATED BY ',' /* columns separated by commas*/
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES /*ignore 1st line: headers*/
(id, title, type, publication_year, issue, volume, chapter, @venue_name, venue_type, @publisher_id, event)
SET venue_id /*set the "venue_id" column in the "Publications" table*/= (SELECT id FROM Venues WHERE name = @venue_name),
    publisher_id = (SELECT id FROM Publishers WHERE id = @publisher_id);




