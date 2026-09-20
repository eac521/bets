#!/bin/zsh
DB=/Users/ericcoxon/code/bets/nba/data/database/nba.db
DEST=/Users/ericcoxon/Dropbox/backups/database
/usr/bin/sqlite3 "$DB" .dump | /usr/bin/gzip > "$DEST/nba_$(date +%F).sql.gz"
/usr/bin/find "$DEST" -name 'nba_*.sql.gz' -mtime +14 -delete
echo "backup completed $(date)"
