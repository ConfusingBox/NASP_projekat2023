// -Potrebno je omoguciti da korisnik podesava osobine memtable-a. Za to nam fali config.json fajl.
// -Potrebno je omoguciti i implementaciju sa skip listom. Za to nam fali SkipList.go fajl.


package strukture

include (
	"fmt"
)


struct Memtable {
	treshold int // Bazirano na ukupnoj memoriji zapisa, ne broju razlicitih zapisa.
	size int
	currentSize int

	data[int]byte map
	data *BTree
	// data *SkipList - Dodati naknadno, kada neko uradi skip listu
}

struct NewMemtable() {
	// Kreira memtable. config.json govori koristi li se mapa ili b-stablo.
}

func add() {
	// Dodaje podatak u memtable.
}

func delete() {
	// Brise podatak iz memtable-a.
}

func get() {
	// Vraca podatak iz memtable-a.
}

func initializeMemtable() {
	// Poziva se pri kreiranju memtable-a. Popunjava ga zapisima iz WAL-a.
	// Iskreno, nemam pojma sta to znaci. :/
}

func flush() {
	// Poziva se kada treshold >= size. (Moze li biti vece ili mora striktno jednako?
	// Sta se desava kada je jedan zapis toliko velik da ne stane ni u prazan memtable?)
}

func createSSTable() {
	// Podatke zapisuje u sstable kada se memtable popuni.
	// Moze se uraditi na vise nacina:
	//	1. Memtable se prazni i ponovo koristi;
	//	2. Pravi se novi memtable i brise se stari;
	//	3. Pravi se novi memtable i rotira se sa starim.
	// Cekamo da neko prvo napravi SSTable.go.
}

func main() {
	// Za testiranje.
}