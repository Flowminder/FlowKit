/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
PUBLIC ---------------------------------------------

Here we define the public schema. This schema is
designed for the purposes of keeping track
of the ingestion of text files.

Available columns:

  - files:  where information about the ingestion of
            files is kept.

----------------------------------------------------
*/
CREATE TABLE IF NOT EXISTS public.files(
    
    id TEXT PRIMARY KEY,
    full_path TEXT,
    directory_path TEXT,
    specification_path TEXT,
    target_table TEXT,
    created TIMESTAMPTZ,
    processed BOOLEAN,
    datetime TIMESTAMPTZ,
    size NUMERIC,
    status TEXT,
    record_number NUMERIC,
    log TEXT

    );
