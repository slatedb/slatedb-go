### ADR Documentation

This directory organizes decisions and proposals using ADR style. For explanation of ADRs and how they are 
intended to be used See [Documenting Architecture Decisions](https://cognitect.com/blog/2011/11/15/documenting-architecture-decisions)

Adding or Superseding existing ADRs can be done easily through the use of the ADR tool

### ADR Tool Usage
[Install adr-tools](https://github.com/npryce/adr-tools).

Use the `adr` command to manage ADRs.  Try running `adr help`.

ADRs are stored in a subdirectory of your project as Markdown files.
The directory for this project is `docs/adr`

1. Create Architecture Decision Records

        adr new Implement as Unix shell scripts

   This will create a new, numbered ADR file and open it in your
   editor of choice (as specified by the VISUAL or EDITOR environment
   variable).

   To create a new ADR that supercedes a previous one (ADR 9, for example),
   use the -s option.

        adr new -s 9 Use Rust for performance-critical functionality

   This will create a new ADR file that is flagged as superceding
   ADR 9, and changes the status of ADR 9 to indicate that it is
   superceded by the new ADR.  It then opens the new ADR in your
   editor of choice.

2. For further information, use the built in help:

        adr help
   



