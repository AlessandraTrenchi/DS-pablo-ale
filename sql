CREATE DATABASE mydatabase; /*creating the database*/()
USE mydatabase;

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
CREATE TABLE Organization (
    id INT PRIMARY KEY AUTO_INCREMENT,
    identifiableEntityId VARCHAR(255),
    name VARCHAR(255) NOT NULL, 
    FOREIGN KEY (identifiableEntityId) REFERENCES IdentifiableEntity (id)
);

CREATE TABLE Publisher (
    id INT PRIMARY KEY AUTO_INCREMENT,
    identifiableEntityId VARCHAR(255),
    FOREIGN KEY (identifiableEntityId) REFERENCES IdentifiableEntity (id)
);

CREATE TABLE Venue (
    id INT PRIMARY KEY AUTO_INCREMENT,
    identifiableEntityId VARCHAR(255),
    organizationName VARCHAR(255),
    title VARCHAR(255),
    FOREIGN KEY (identifiableEntityId) REFERENCES IdentifiableEntity (id),
    FOREIGN KEY (organizationId) REFERENCES Organization (id)
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
    id INT PRIMARY KEY AUTO_INCREMENT,
    venueId INT,
    FOREIGN KEY (venueId) REFERENCES Venue (id)
);

CREATE TABLE BookChapter (
    id INT PRIMARY KEY AUTO_INCREMENT,
    publicationId INT,
    chapterNumber INT NOT NULL,
    FOREIGN KEY (publicationId) REFERENCES Publication (id)
);
CREATE TABLE JournalArticle (
    id INT PRIMARY KEY AUTO_INCREMENT,
    publicationId INT,
    issue VARCHAR (255),
    volume VARCHAR (255),
    FOREIGN KEY (PublicationId) REFERENCES Publication (id)
);







CREATE TABLE Publication (
    doi VARCHAR(255) PRIMARY KEY, /*Data type VARCHAR holding upto 255 characters, this column will be the primary row in the table*/
    title VARCHAR(255), /*column name*/
    type VARCHAR (50), /*journal, article, bookchapter, other */
    publication_year INT, 
    issue VARCHAR(50),
    volume VARCHAR(50),
    chapter VARCHAR(50),
    venue_id INT,
    publisher_id INT,
    FOREIGN KEY (venue_id) REFERENCES Venues(id), /*defines a relationship between the venue_id of the Publications table and the id of the Venues table*/
    FOREIGN KEY (publisher_id) REFERENCES Publishers (id)
);