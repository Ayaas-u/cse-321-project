/*
 * journal.c - Metadata Journaling (PDF-accurate skeleton)
 *
 * Commands:
 *   ./journal create <filename>
 *   ./journal install
 *
 * IMPORTANT (from PDF):
 * - Journal is 16 blocks and treated as an append-only byte array.
 * - journal_header is fixed at offset 0 of the journal region.
 * - "empty journal" means nbytes_used == sizeof(journal_header).
 * - rec_header is { uint16_t type; uint16_t size; }.
 * - DATA record logs one full 4096-byte block image + home block_no.
 * - COMMIT record seals one transaction (just header).
 *
 * We do ONLY what the PDF asks. No extra journaling features.
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>

/* =========================
 *        DISK LAYOUT (PDF)
 * =========================
 * Block size: 4 KB
 *
 * Block 0:  Superblock (1 block)
 * Blocks 1-16: Journal (16 blocks)
 * Block 17: Inode Bitmap (1 block)
 * Block 18: Data Bitmap  (1 block)
 * Blocks 19-20: Inode Table (2 blocks)
 * Blocks 21-84: Data Blocks (64 blocks)
 */

#define BLOCK_SIZE           4096
#define TOTAL_BLOCKS         85

#define SUPERBLOCK_BLK       0

#define JOURNAL_START_BLK    1
#define JOURNAL_NBLOCKS      16
#define JOURNAL_BYTES        (JOURNAL_NBLOCKS * BLOCK_SIZE)

#define INODE_BMAP_BLK       17
#define DATA_BMAP_BLK        18
#define INODE_TBL_START_BLK  19
#define INODE_TBL_NBLOCKS    2
#define DATA_START_BLK       21

/* =========================
 *        JOURNAL SPEC (PDF)
 * ========================= */

#define JOURNAL_MAGIC 0x4A524E4C   /* "JRNL" */
#define REC_DATA      1
#define REC_COMMIT    2

struct journal_header {
    uint32_t magic;        /* store JOURNAL_MAGIC */
    uint32_t nbytes_used;  /* total bytes currently used in journal byte-array */
};

struct rec_header {
    uint16_t type;         /* REC_DATA or REC_COMMIT */
    uint16_t size;         /* total record size in bytes (including this header) */
};

/* DATA record (PDF):
 * struct data_record {
 *   struct rec_header hdr;   // type = REC_DATA
 *   uint32_t block_no;       // absolute home block index in disk image
 *   uint8_t data[4096];      // full block image
 * };
 *
 * Total size = sizeof(rec_header) + sizeof(uint32_t) + 4096
 */
#define DATA_REC_SIZE (sizeof(struct rec_header) + sizeof(uint32_t) + BLOCK_SIZE)

/* COMMIT record (PDF): seals one transaction.
 * In this project: just the rec_header with type=REC_COMMIT.
 */
#define COMMIT_REC_SIZE (sizeof(struct rec_header))

/* =========================
 *        BASIC HELPERS
 * ========================= */

static void die(const char *msg) {
    perror(msg);
    exit(1);
}

static off_t blk_off(uint32_t blkno) {
    return (off_t)blkno * (off_t)BLOCK_SIZE;
}

/* Read/write full blocks (home blocks on disk) */
static void read_block(int fd, uint32_t blkno, void *buf) {
    if (lseek(fd, blk_off(blkno), SEEK_SET) < 0) die("lseek(read_block)");
    ssize_t n = read(fd, buf, BLOCK_SIZE);
    if (n != BLOCK_SIZE) die("read_block");
}

static void write_block(int fd, uint32_t blkno, const void *buf) {
    if (lseek(fd, blk_off(blkno), SEEK_SET) < 0) die("lseek(write_block)");
    ssize_t n = write(fd, buf, BLOCK_SIZE);
    if (n != BLOCK_SIZE) die("write_block");
}

/* =========================
 *    JOURNAL BYTE-ARRAY I/O
 * =========================
 * Journal is a byte array of size JOURNAL_BYTES starting at block JOURNAL_START_BLK.
 * journal_header is at offset 0 within this region.
 */

static off_t journal_base_off(void) {
    return blk_off(JOURNAL_START_BLK);
}

static void journal_read_header(int fd, struct journal_header *jh) {
    if (lseek(fd, journal_base_off(), SEEK_SET) < 0) die("lseek(journal_read_header)");
    ssize_t n = read(fd, jh, sizeof(*jh));
    if (n != (ssize_t)sizeof(*jh)) die("read(journal_header)");
}

static void journal_write_header(int fd, const struct journal_header *jh) {
    if (lseek(fd, journal_base_off(), SEEK_SET) < 0) die("lseek(journal_write_header)");
    ssize_t n = write(fd, jh, sizeof(*jh));
    if (n != (ssize_t)sizeof(*jh)) die("write(journal_header)");
}

/* Append bytes into journal at current nbytes_used (must update header yourself) */
static void journal_append_bytes(int fd, uint32_t nbytes_used, const void *src, uint32_t len) {
    /* bounds check: must not exceed 16 blocks */
    if ((uint64_t)nbytes_used + (uint64_t)len > (uint64_t)JOURNAL_BYTES) {
        fprintf(stderr, "journal full: append would exceed %u bytes\n", JOURNAL_BYTES);
        exit(1);
    }
    off_t off = journal_base_off() + (off_t)nbytes_used;
    if (lseek(fd, off, SEEK_SET) < 0) die("lseek(journal_append_bytes)");
    ssize_t n = write(fd, src, len);
    if (n != (ssize_t)len) die("write(journal_append_bytes)");
}

/* Read bytes from journal (used by install scan) */
static void journal_read_bytes(int fd, uint32_t offset, void *dst, uint32_t len) {
    if ((uint64_t)offset + (uint64_t)len > (uint64_t)JOURNAL_BYTES) {
        fprintf(stderr, "journal read out of bounds\n");
        exit(1);
    }
    off_t off = journal_base_off() + (off_t)offset;
    if (lseek(fd, off, SEEK_SET) < 0) die("lseek(journal_read_bytes)");
    ssize_t n = read(fd, dst, len);
    if (n != (ssize_t)len) die("read(journal_read_bytes)");
}

/* Initialize journal if not initialized */
static void journal_init_if_needed(int fd) {
    struct journal_header jh;
    journal_read_header(fd, &jh);

    if (jh.magic != JOURNAL_MAGIC) {
        jh.magic = JOURNAL_MAGIC;
        jh.nbytes_used = (uint32_t)sizeof(struct journal_header); /* empty journal rule (PDF) */
        journal_write_header(fd, &jh);
    }
}

/* =========================
 *  PART A (YOU): create
 * =========================
 * Required behavior (PDF):
 * - Compute updated metadata blocks for creating a file in root directory (in memory).
 * - Append DATA records for ONLY the modified metadata blocks.
 * - Append one COMMIT record.
 * - Update journal_header.nbytes_used.
 * - DO NOT write modified metadata to their home locations here.
 *
 * NOTE: This skeleton does NOT invent VSFS struct details.
 * You will fill in the VSFS-specific metadata updates based on the provided project code/headers.
 */

static void handle_create(int fd, const char *filename) {
    journal_init_if_needed(fd);

    struct journal_header jh;
    journal_read_header(fd, &jh);

    /* ===== YOUR TODO: compute which METADATA blocks change for create =====
     * Likely modified blocks (PDF examples):
     * - inode bitmap block (INODE_BMAP_BLK)
     * - inode table block(s) (INODE_TBL_START_BLK ..)
     * - root directory data block (some block in DATA region, pointed by root inode)
     *
     * You must:
     * 1) read original blocks from disk into 4096-byte buffers
     * 2) modify those buffers in memory to reflect "create filename"
     */

    /* Example buffers (you will actually use the ones you modify): */
    uint8_t inode_bmap[BLOCK_SIZE];
    uint8_t inode_tbl_blk0[BLOCK_SIZE];
    uint8_t root_dir_blk[BLOCK_SIZE];

    /* Read originals (YOU may need to read correct inode table block(s) + root dir block) */
    read_block(fd, INODE_BMAP_BLK, inode_bmap);
    read_block(fd, INODE_TBL_START_BLK, inode_tbl_blk0);
    /* root_dir_blk read: YOU must locate correct root directory block number */
    /* read_block(fd, <ROOT_DIR_HOME_BLOCK_NO>, root_dir_blk); */

    /* ===== YOUR TODO: modify inode_bmap, inode_tbl_blk0, root_dir_blk in memory ===== */

    /* ===== Append DATA records for each modified metadata block ===== */
    uint32_t used = jh.nbytes_used;

    /* Helper: write one DATA record (header + block_no + 4096 bytes) */
    auto append_data = [&](uint32_t home_block_no, uint8_t *block_image) {
        struct rec_header rh;
        rh.type = REC_DATA;
        rh.size = (uint16_t)DATA_REC_SIZE;

        journal_append_bytes(fd, used, &rh, sizeof(rh));
        used += sizeof(rh);

        journal_append_bytes(fd, used, &home_block_no, sizeof(home_block_no));
        used += sizeof(home_block_no);

        journal_append_bytes(fd, used, block_image, BLOCK_SIZE);
        used += BLOCK_SIZE;
    };

    /* NOTE: C doesn’t have lambdas; replace the above with a real helper function
       if your compiler complains. (If using gcc/clang, delete lambda & write a function.) */

    /* ===== YOUR TODO: call append_data() for each modified metadata block ===== */
    /* append_data(INODE_BMAP_BLK, inode_bmap); */
    /* append_data(INODE_TBL_START_BLK, inode_tbl_blk0); */
    /* append_data(<ROOT_DIR_HOME_BLOCK_NO>, root_dir_blk); */

    /* ===== Append COMMIT record ===== */
    {
        struct rec_header rh;
        rh.type = REC_COMMIT;
        rh.size = (uint16_t)COMMIT_REC_SIZE;
        journal_append_bytes(fd, used, &rh, sizeof(rh));
        used += sizeof(rh);
    }

    /* Update header */
    jh.nbytes_used = used;
    journal_write_header(fd, &jh);

    printf("create: journaled metadata for '%s'\n", filename);
}

/* =========================
 *  PART B (HIM): install
 * =========================
 * Required behavior (PDF):
 * - Scan journal records up to nbytes_used
 * - For each transaction that has a COMMIT:
 *     replay every logged DATA record by writing its 4096-byte image to its home block number
 * - After replaying all committed transactions:
 *     clear (checkpoint) journal so it becomes empty again
 *     -> set nbytes_used = sizeof(journal_header)
 */

static void handle_install(int fd) {
    journal_init_if_needed(fd);

    struct journal_header jh;
    journal_read_header(fd, &jh);

    if (jh.nbytes_used == sizeof(struct journal_header)) {
        printf("install: journal empty\n");
        return;
    }

    /* ===== HIS TODO: scan records from offset = sizeof(journal_header) to jh.nbytes_used =====
     * Strategy:
     * - temp list/array of DATA records for current transaction
     * - when COMMIT seen: replay all stored DATA records (write_block to home block), then clear temp list
     * - if reach end without COMMIT: discard temp list
     *
     * Must use rec_header.size to advance.
     * Must validate sizes so you never read past jh.nbytes_used.
     */

    fprintf(stderr, "install: NOT IMPLEMENTED YET (partner implements)\n");
}

/* =========================
 *            MAIN
 * ========================= */

static void usage(const char *p) {
    fprintf(stderr,
        "Usage:\n"
        "  %s create <filename>\n"
        "  %s install\n", p, p);
    exit(1);
}

int main(int argc, char **argv) {
    if (argc < 2) usage(argv[0]);

    int fd = open("vsfs.img", O_RDWR);
    if (fd < 0) die("open(vsfs.img)");

    if (strcmp(argv[1], "create") == 0) {
        if (argc != 3) usage(argv[0]);
        handle_create(fd, argv[2]);
    } else if (strcmp(argv[1], "install") == 0) {
        if (argc != 2) usage(argv[0]);
        handle_install(fd);
    } else {
        usage(argv[0]);
    }

    close(fd);
    return 0;
}
