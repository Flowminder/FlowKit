# Asciidoctor PDF for converting Jupyter notebooks to PDF

Date: 22 November 2019

# Status

Pending

## Context

We want AutoFlow to support creation of PDF reports from Jupyter notebooks. [nbconvert](https://nbconvert.readthedocs.io/en/latest/) provides a method for converting Jupyter notebooks to PDF format. However, PDF conversion with nbconvert requires a full LaTeX installation, which is ~4GB in size. To keep the size of the AutoFlow container smaller, it is desirable to find an alternative that doesn't require a LaTeX installation.

[Asciidoctor PDF](https://asciidoctor.org/docs/asciidoctor-pdf/) is a tool for converting ASCIIDoc documents to PDF format without generating an interim format such as LaTeX. Since nbconvert can convert notebooks to ASCIIDoc format, we can use Asciidoctor PDF as the second half of a two-step process to convert Jupyter notebooks to PDF via ASCIIDoc.

## Decision

We will use a two-step process to convert Jupyter notebooks to PDF reports in AutoFlow: convert the notebook to ASCIIDoc format using nbconvert, and then convert the resulting ASCIIDoc document to PDF using Asciidoc PDF.

## Consequences

The AutoFlow docker image does not require a full LaTeX installation, which would increase the image size by ~4GB (current image size is <1GB).

Asciidoctor PDF is a Ruby package, so this adds a non-python dependency to AutoFlow.

If a user automates a notebook that produces LaTeX outputs (e.g. equations), these will not be displayed properly in the resulting PDF.
