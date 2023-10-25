CREATE DATABASE pabloale;
USE pabloale;

-- Higher level classes representing the entity-relationship model of the data

/* Entities from CSV data */


CREATE TABLE Event (/*event table is a property of Publication*/
    event_id INT PRIMARY KEY AUTO_INCREMENT,
    event_detail VARCHAR(50),
    UNIQUE (event_id)
);

CREATE TABLE Publication (
    publication_id VARCHAR(255) PRIMARY KEY,
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
    UNIQUE (publication_id),
    FOREIGN KEY (publisher_id) REFERENCES Publisher(publisher_id),
    FOREIGN KEY (event_id) REFERENCES Event(event_id)
    FOREIGN KEY (identifiable_entity_id) REFERENCES Identifiable_Entity(identifiable_entity_id)

);


/* Entities from JSON structure and reference UML */
CREATE TABLE Identifiable_Entity ( /*probably an auxiliary table, maybe DOI inside Venue what if there is a hierarchical relationship? in the sense that identifiableEntity/DOI contains */
    identiable_entity_id VARCHAR(50) PRIMARY KEY,
    UNIQUE (identiable_entity_id)
);

CREATE TABLE Person (
    person_id INT PRIMARY KEY AUTO_INCREMENT,
    identifiable_entity_id VARCHAR(50) REFERENCES Identifiable_Entity(identifiable_entity_id),
    UNIQUE (person_id),
    family VARCHAR(50) NOT NULL,
    given VARCHAR(50) NOT NULL
);


CREATE TABLE Venue (
    venues_id INT PRIMARY KEY AUTO_INCREMENT,
    identifiable_entity_id VARCHAR(50),
    title VARCHAR(255),
    type VARCHAR(50) NOT NULL,
    UNIQUE (venues_id),
    FOREIGN KEY (identifiable_entity_id) REFERENCES Identifiable_Entity(identifiable_entity_id)
);


CREATE TABLE Organization ( /*auxiliary table or part of Publishers*/
    organization_id INT PRIMARY KEY AUTO_INCREMENT,
    identifiable_entity_id VARCHAR(50) REFERENCES Identifiable_Entity(identifiable_entity_id),
    UNIQUE (organization_id),
    name VARCHAR(255) NOT NULL
);

/**CREATE TABLE Publication_Venue (
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id),
    VenueId INT REFERENCES Venue(id)
);**/

/**CREATE TABLE Citation (
    id INT PRIMARY KEY AUTO_INCREMENT,
    CitingPublicationId VARCHAR(255) REFERENCES Publication(id),
    CitedPublicationId VARCHAR(255) REFERENCES Publication(id)
);**/

CREATE TABLE Book_Chapter (
    book_chapter_id INT PRIMARY KEY AUTO_INCREMENT,
    publication_id VARCHAR(255) REFERENCES Publication(publication_id),
    UNIQUE (book_chapter_id),
    chapterNumber INT NOT NULL
);

CREATE TABLE Book (
    book_id INT PRIMARY KEY AUTO_INCREMENT,
    venue_id INT,
    FOREIGN KEY (venue_id) REFERENCES Venue(venue_id)
);


CREATE TABLE Journal_Article (
    journal_article_id INT PRIMARY KEY AUTO_INCREMENT,
    UNIQUE (journal_article_id),
    publication_id VARCHAR(255) REFERENCES Publication(publication_id)
);

CREATE TABLE Journal (
    journal_id INT PRIMARY KEY AUTO_INCREMENT,
    venue_id INT,
    FOREIGN KEY (venue_id) REFERENCES Venue(venue_id)
);


CREATE TABLE Proceedings (
    proceedings_id INT PRIMARY KEY AUTO_INCREMENT,
    event VARCHAR(255) NOT NULL,
    venue_id INT,
    FOREIGN KEY (venue_id) REFERENCES Venue(venue_id)
);

CREATE TABLE Proceedings_Paper (
    proceedings_id INT PRIMARY KEY AUTO_INCREMENT,
    UNIQUE (proceedings_id),
    publication_id VARCHAR(255) REFERENCES Publication(publication_id)
);


-- from JSON

CREATE TABLE Authors ( /*same name as in the dataset, but there it is not camel case*/
    authors_id SERIAL PRIMARY KEY, /*forse dovrebbe essere doi, which contains family given and orchid inside*/
    family VARCHAR(100) NOT NULL,
    given VARCHAR(100) NOT NULL,
    orcid VARCHAR(50) NOT NULL,
    publication_id VARCHAR(255) NOT NULL,
    UNIQUE (id),
    FOREIGN KEY (publication_id) REFERENCES Publication(publication_id)
);

CREATE TABLE Publication ( /*could be inside authors or venues_id*/
    publication_id VARCHAR(255) PRIMARY KEY,
    doi VARCHAR(50) NOT NULL,
    UNIQUE (id, doi)
);

CREATE TABLE References ( /*useful to establish relationships between publications*/
    references_id SERIAL PRIMARY KEY,
    source_doi VARCHAR(255) NOT NULL,
    target_doi VARCHAR(255) NOT NULL,
    UNIQUE (id, doi)
    FOREIGN KEY (source_doi) REFERENCES Publication(doi),
    FOREIGN KEY (target_doi) REFERENCES Publication(doi)
);

CREATE TABLE Publishers ( /*same name as in the json file, but with capital case here*/
    publishers_id VARCHAR(255) PRIMARY KEY,
    UNIQUE (id)
    name VARCHAR(255) NOT NULL
);

-- Table to establish the relationship between Authors and Publications
/* CREATE TABLE Author_Publication (
    id SERIAL PRIMARY KEY,
    author_id INT REFERENCES Authors(id),
    publication_id INT REFERENCES Publications(id)
); 


