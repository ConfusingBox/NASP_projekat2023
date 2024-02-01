// -Potrebno je omoguciti da korisnik podesava osobine memtable-a. Za to nam fali config.json fajl.
// -Potrebno je omoguciti i implementaciju sa skip listom. Za to nam fali SkipList.go fajl.

package strukture

import (
	"errors"

	"github.com/go-delve/delve/pkg/config"
)

type Memtable struct {
	threshold   float32
	size        int
	currentSize int

	dataType     string
	dataHashMap  map[string][]byte
	dataSkipList *SkipList
	dataBTree    *BTree
}

// NewMemtable creates a new Memtable based on the specified data type.
func NewMemtable() (*Memtable, error) {
	config, err := config.LoadConfig("config.json")
	if err != nil {
		return nil, err
	}

	mt := Memtable{config.MemTableThreshold, config.MemTableSize, 0, config.MemTableType, make(map[string][]byte), NewSkipList(config.SkipListDepth), NewBTree(config.BTreeDegree)}
	return &mt, nil
}

func (mt *Memtable) InsertSkipList(key string, value []byte) error {
	exist := mt.dataSkipList.Search([]byte(key))

	if exist == nil {
		mt.dataSkipList.Insert([]byte(key), value)
		mt.currentSize += 1

		return nil
	}
	return errors.New("Same key already here lol")
}

func (mt *Memtable) InsertBTree(key string, value []byte) error {
	err := mt.dataBTree.Insert([]byte(key), value)

	if err != nil {
		return err
	}

	mt.currentSize += 1
	return nil
}

func (mt *Memtable) InsertHashMap(key string, value []byte) error {
	_, exist := mt.dataHashMap[key]

	if exist {
		return errors.New("Same key already here lol")
	}

	mt.dataHashMap[key] = value
	mt.currentSize += 1
	return nil
}

func (mt *Memtable) add(key int, value []byte) {
	// Dodaje podatak u memtable.
	//proveri da li imas prostora da se doda

	// currentSize ukupnu velicina u memoriji
	// ako nema mesta da se doda onda sta treba da se uradi
	// ako je se dodaje u popunjen memtable //	1. Memtable se prazni i ponovo koristi;
}

// func (mt *Memtable) delete(key int) {
// 	// Briše podatak iz memtable-a.
// 	if mt.data1 != nil {
// 		if _, exists := mt.data1[key]; exists {
// 			delete(mt.data1, key)
// 			mt.currentSize--
// 		} else {
// 			fmt.Printf("Kljuc %d ne postoji u memtable\n", key)
// 		}
// 	} else {
// 		fmt.Println("Memtable je prazan, ne mozes brisati podatke")
// 	}
// }

// func (mt *Memtable) get(key int) (byte, bool) {
// 	// Vraća podatak iz memtable-a.
// 	if mt.data1 != nil {
// 		if value, exists := mt.data1[key]; exists {
// 			return value, true
// 		} else {
// 			fmt.Printf("Kljuc %d ne postoji u memtable\n", key)
// 			return 0, false
// 		}
// 	} else {
// 		fmt.Println("Memtable je prazan, ne mozes GET")
// 		return 0, false
// 	}
// }

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
