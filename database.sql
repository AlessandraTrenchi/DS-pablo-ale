CREATE DATABASE pabloale;
USE pabloale;

-- Higher level classes representing the entity-relationship model of the data

/* Entities from CSV data */


CREATE TABLE Event (/*event table is a property of Publication*/
    id INT PRIMARY KEY AUTO_INCREMENT,
    event_detail VARCHAR(50),
    UNIQUE (id)
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
    publisher VARCHAR(50),
    event INT,
    UNIQUE (id),
    FOREIGN KEY (publisher) REFERENCES Publisher(id),
    FOREIGN KEY (event_id) REFERENCES Event(id)
);

/* Entities from JSON structure and reference UML */
CREATE TABLE Identifiable_Entity ( /*probably an auxiliary table, maybe DOI inside Venue what if there is a hierarchical relationship? in the sense that identifiableEntity/DOI contains */
    id VARCHAR(50) PRIMARY KEY,
    UNIQUE (id)
);

CREATE TABLE Person ( /* properties of Authors*/
    id INT PRIMARY KEY AUTO_INCREMENT,
    identifiableEntityId VARCHAR(50) REFERENCES Identifiable_Entity(id),
    UNIQUE (id),
    family VARCHAR(50) NOT NULL,
    given VARCHAR(50) NOT NULL
    );

CREATE TABLE Venue ( /*it's a part of Publication-*/
    id INT PRIMARY KEY AUTO_INCREMENT,
    IdentifiableEntityId VARCHAR(50) REFERENCES Identifiable_Entity(id),
    title VARCHAR(255),
    UNIQUE (id),
    type VARCHAR(50) NOT NULL
);

CREATE TABLE Organization ( /*auxiliary table or part of Publishers*/
    id INT PRIMARY KEY AUTO_INCREMENT,
    IdentifiableEntityId VARCHAR(50) REFERENCES Identifiable_Entity(id),
    UNIQUE (id),
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

CREATE TABLE Book_Chapter ( /*type parameter of Publication*/
    id INT PRIMARY KEY AUTO_INCREMENT,
    PublicationId VARCHAR(255) REFERENCES Publication(id),
    UNIQUE (id),
    chapterNumber INT NOT NULL
);

CREATE TABLE Journal_Article ( /*type parameter of publication*/
    id INT PRIMARY KEY AUTO_INCREMENT,
    UNIQUE (id),
    PublicationId VARCHAR(255) REFERENCES Publication(id)
);

CREATE TABLE Proceedings_Paper ( /*maybe auxiliary table because there are no type of publication called like this */
    id INT PRIMARY KEY AUTO_INCREMENT,
    UNIQUE (id),
    PublicationId VARCHAR(255) REFERENCES Publication(id)
);

-- from JSON

CREATE TABLE Authors ( /*same name as in the dataset, but there it is not camel case*/
    id SERIAL PRIMARY KEY, /*forse dovrebbe essere doi, which contains family given and orchid inside*/
    family VARCHAR(100) NOT NULL,
    given VARCHAR(100) NOT NULL,
    orcid VARCHAR(50) NOT NULL,
    publication_id VARCHAR(255) NOT NULL,
    UNIQUE (id),
    FOREIGN KEY (publication_id) REFERENCES Publication(id)
);

CREATE TABLE Publications ( /*could be inside authors or venues_id*/
    id VARCHAR(255) PRIMARY KEY,
    doi VARCHAR(50) NOT NULL,
    UNIQUE (id, doi)
);

CREATE TABLE References ( /*useful to establish relationships between publications*/
    id SERIAL PRIMARY KEY,
    source_doi VARCHAR(255) NOT NULL,
    target_doi VARCHAR(255) NOT NULL,
    UNIQUE (id, doi)
    FOREIGN KEY (source_doi) REFERENCES Publications(doi),
    FOREIGN KEY (target_doi) REFERENCES Publications(doi)
);

CREATE TABLE Publishers ( /*same name as in the json file, but with capital case here*/
    id VARCHAR(255) PRIMARY KEY,
    UNIQUE (id)
    name VARCHAR(255) NOT NULL
);

-- Table to establish the relationship between Authors and Publications
/* CREATE TABLE Author_Publication (
    id SERIAL PRIMARY KEY,
    author_id INT REFERENCES Authors(id),
    publication_id INT REFERENCES Publications(id)
); 

-- Populating the tables in populate.py
