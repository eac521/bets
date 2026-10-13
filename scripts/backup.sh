#!/bin/zsh
set -o pipefail
DB=/Users/ericcoxon/code/bets/nba/data/database/nba.db
DEST=/Users/ericcoxon/Dropbox/backups/database
# one file, overwritten nightly; Dropbox version history keeps the older copies
# dump to a temp file first so a failed dump never replaces the last good backup
/usr/bin/sqlite3 "$DB" .dump | /usr/bin/gzip > "$DEST/nba.sql.gz.tmp" && mv "$DEST/nba.sql.gz.tmp" "$DEST/nba.sql.gz" || { echo "backup FAILED $(date)" >&2; exit 1; }
echo "backup completed $(date)"
