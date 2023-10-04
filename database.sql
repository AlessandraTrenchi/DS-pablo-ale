CREATE DATABASE pabloale;
USE pabloale;

-- Higher level classes representing the entity-relationship model of the data

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
CREATE TABLE Identifiable_Entity (
    id VARCHAR(50) PRIMARY KEY
);

CREATE TABLE Person (
    id INT PRIMARY KEY AUTO_INCREMENT,
    identifiableEntityId VARCHAR(50) REFERENCES Identifiable_Entity(id),
    givenName VARCHAR(50) NOT NULL,
    familyName VARCHAR(50) NOT NULL
);

CREATE TABLE Venue (
    id INT PRIMARY KEY AUTO_INCREMENT,
    IdentifiableEntityId VARCHAR(50) REFERENCES Identifiable_Entity(id),
    title VARCHAR(255),
    type VARCHAR(50) NOT NULL
);

CREATE TABLE Organization (
    id INT PRIMARY KEY AUTO_INCREMENT,
    IdentifiableEntityId VARCHAR(50) REFERENCES Identifiable_Entity(id),
    name VARCHAR(255) NOT NULL
);

CREATE TABLE Publication_Venue (
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id),
    VenueId INT REFERENCES Venue(id)
);

CREATE TABLE Citation (
    id INT PRIMARY KEY AUTO_INCREMENT,
    CitingPublicationId VARCHAR(255) REFERENCES Publication(id),
    CitedPublicationId VARCHAR(255) REFERENCES Publication(id)
);

CREATE TABLE Book_Chapter (
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id),
    chapterNumber INT NOT NULL
);

CREATE TABLE Journal_Article (
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id)
);

CREATE TABLE Proceedings_Paper (
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id)
);

-- from JSON

CREATE TABLE Authors (
    id SERIAL PRIMARY KEY,
    family VARCHAR(100) NOT NULL,
    given VARCHAR(100) NOT NULL,
    orcid VARCHAR(50) NOT NULL,
    publication_id VARCHAR(255) NOT NULL,
    FOREIGN KEY (publication_id) REFERENCES Publication(id)
);

CREATE TABLE Publications (
    id VARCHAR(255) PRIMARY KEY,
    doi VARCHAR(50) NOT NULL
);

CREATE TABLE References (
    id SERIAL PRIMARY KEY,
    source_doi VARCHAR(255) NOT NULL,
    target_doi VARCHAR(255) NOT NULL,
    FOREIGN KEY (source_doi) REFERENCES Publications(doi),
    FOREIGN KEY (target_doi) REFERENCES Publications(doi)
);

CREATE TABLE Publishers (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Table to establish the relationship between Authors and Publications
CREATE TABLE Author_Publication (
    id SERIAL PRIMARY KEY,
    author_id INT REFERENCES Authors(id),
    publication_id INT REFERENCES Publications(id)
);

-- Populating the tables in populate.py
