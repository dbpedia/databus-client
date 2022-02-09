# Concept

The Databus Client is designed in a modular way to achieve high reusability, which means that the components and functionalities such as the downloading component, and compression converter can be used separately and interchangeably. It leverages 4 functionality layers depicted in the figure below.

<img src="../img/concept.png" alt="Databus Client Concept"/>

## Download-Layer
The fundamental **Download-Layer** is supposed to download exact copies of data assets via the DBpedia Databus in a flexible way.
It can be understood as a simple extraction phase of the ETL (Extract-Transform-Load) process.
Moreover, it is supposed to persist the input data provenance by recording stable file identifiers and additional metadata.
The data assets to be downloaded can be selected in a fine-grained way via an interoperable data dependency specification. and optional compiling configurations tailored to the needs of a consuming app or workflow.

## Compression-Layer
If any conversion process is required, the **Compression-Layer** takes action. It sniffs for the input compression format and decompresses the file. If the input file format differs from the output file format, the decompressed file is passed to the Format-Layer.
The Compression-Layer takes the decompressed file, which may be format converted by the Format-Layer or Mapping-Layer, and compresses it to the requested output compression format. This compressed file is passed back to the Download-Layer, after the conversion process has finished.


## File-Format-Layer
Within the data format conversion process, the Databus Client utilizes the Format-layer and the Mapping-Layer where required.
The **Format-Layer** receives the uncompressed file and parses it to a unified internal data structure of the corresponding (format) equivalence class.
Such an equivalence class contains all serialization formats that can be used interchangeably while representing the same amount of information, given a defined common data model for the class (e.g. a set of triples for RDF triple formats, a table of Strings for tabular-structured data formats).
Subsequently, the Format-Layer serializes the internal data structure to the desired output file format.
It passes the serialized data back to the Compression-Layer.

## Mapping-Layer
Whenever the input file format and the requested output file format are in different equivalence classes (e.g. Turtle/RDF triples and TSV/tabular-structured data), the \textbf{Mapping-Layer} is additionally used.
However, it could also be used to manipulate the data of the same equivalence class (e.g. ontology mapping).
With the aid of mapping configurations, the Mapping-Layer transforms the data represented using the internal data structure of the input equivalence class, to data of the internal data structure of the equivalence class of the target file format.
After that process has finished, the data is passed back to the Format layer.

The Compression-Layer, File-Format-Layer, and Mapping-Layer represent the transformation-phase of the ETL process.