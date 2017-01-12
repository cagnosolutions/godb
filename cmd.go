package godb

/*
    Store Methods
    =============
    func OpenStore(path string) (*Store, error)
    func (s *Store) Add(key, val interface{}) error
    func (s *Store) Set(key, val interface{}) error
    func (s *Store) Get(key, ptr interface{}) error
    func (s *Store) Del(key interface{}) error
    func (s *Store) QueryOne(qry string, ptr interface{}) error
    func (s *Store) Query(qry string, ptr interface{}) error
    func (s *Store) Count() int
    func (s *Store) Close() error
    func (s *Store) Sync()

    Interactive Command Line Interface ie. REPL (examples, notes, ideas)
    ====================================================================
    | $ godb -o users 
    | [godb:users]+ Successfully opened the users store     
    | [godb:users]$ insert 123 { Id: 123, Name: "Scott Cagno", Active: true }
    | [godb:users]+ OK    
    | [godb:users]$ insert 123 { Id: 123 }
    | [godb:users]- Entry 123 already exists!
    | [godb:users]$ update 123 { Active: false }
    | [godb:users]+ OK    
    | [godb:users]$ return 123
    | [godb:users]+ { Id: 123, Name: "Scott Cagno", Active: false }
    | [godb:users]$ delete 123
    | [godb:users]+ OK
    | [godb:users]$ query "active == true"
    | [godb:users]- No results found.
    | [godb:users]$ query "active == false" limit 1
    | [godb:users]+ { Id: 123, Name: "Scott Cagno", Active: false }
    | [godb:users]$ count
    | [godb:users]+ 0
`   | [godb:users]$ sync
    | [godb:users]+ OK
    | [godb:users]$ close
    | [godb:users]+ Syncing store and closing.
    | $

    
    NOTE: This implementation opens, performs the action, syncs and
    closes for each command. Leaving no need for any explicit opens, 
    syncs or closes. It also locs around each action, so it's "safe"
    ---------------------------------------------------------------
    Non-Interactive Command Line Interface (examples, notes, ideas)
    ===============================================================
    | $ godb --add users --key 123 --val { Id: 123, Name: "Scott Cagno", Active: true }
    | $ godb --add users --key 123 --val { Id: 123 }
    | err: Entry 123 already exists!
    | $ godb --set users --key 123 --val { Active: false }
    | $ godb --get users --key 123
    | { Id: 123, Name: "Scott Cagno", Active: false }
    | $ godb --del users --key 123 
    | $ godb --qry users "active == true"
    | err: No results found.
    | $ godb --qry users --key 1 --val "active == false" // key acts as limiter
    | { Id: 123, Name: "Scott Cagno", Active: false }
    | $
*/
