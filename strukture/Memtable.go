// -Potrebno je omoguciti da korisnik podesava osobine memtable-a. Za to nam fali config.json fajl.
// -Potrebno je omoguciti i implementaciju sa skip listom. Za to nam fali SkipList.go fajl.

package strukture

import "fmt"

type Memtable struct {
	treshold    int // Bazirano na ukupnoj memoriji zapisa, ne broju razlicitih zapisa.
	size        int
	currentSize int

	data1 map[string][]byte
	// data2 *BTree
	// data *SkipList - Dodati naknadno, kada neko uradi skip listu
}

func NewMemtable(threshold, size int) *Memtable {
	// Kreira memtable. config.json govori koristi li se mapa ili b-stablo.
	mt := Memtable{threshold, size, 0, nil}
	mt.data1 = make(map[string][]byte)
	mt.initializeMemtable()

	return &mt

}

func (mt *Memtable) add(key int, value []byte) {
	// Dodaje podatak u memtable.
	//proveri da li imas prostora da se doda

	mt.data1[key] = value
	mt.currentSize = mt.currentSize + []byte.size()
	// currentSize ukupnu velicina u memoriji
	// ako nema mesta da se doda onda sta treba da se uradi
	// ako je se dodaje u popunjen memtable //	1. Memtable se prazni i ponovo koristi;
}

func (mt *Memtable) delete(key int) {
	// Briše podatak iz memtable-a.
	if mt.data1 != nil {
		if _, exists := mt.data1[key]; exists {
			delete(mt.data1, key)
			mt.currentSize--
		} else {
			fmt.Printf("Kljuc %d ne postoji u memtable\n", key)
		}
	} else {
		fmt.Println("Memtable je prazan, ne mozes brisati podatke")
	}
}

func (mt *Memtable) get(key int) (byte, bool) {
	// Vraća podatak iz memtable-a.
	if mt.data1 != nil {
		if value, exists := mt.data1[key]; exists {
			return value, true
		} else {
			fmt.Printf("Kljuc %d ne postoji u memtable\n", key)
			return 0, false
		}
	} else {
		fmt.Println("Memtable je prazan, ne mozes GET")
		return 0, false
	}
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

// func main() {
// 	// Za testiranje.
// }
