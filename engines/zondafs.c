#include <stddef.h>
#include <math.h>

#include "../fio.h"
#include "../optgroup.h"

/* zonda_fs client headers */
#include "src/file_client/zonda_fs_c.h"

/*
 * ZondaFS I/O Engine for FIO
 *
 * USAGE EXAMPLES:
 *
 * 1. Append Write Test (追加写测试)
 *    Performs sequential write operations with append mode:
 *
 *    ./fio --name=append_test \
 *          --ioengine=zondafs \
 *          --rw=write \
 *          --bs=4k \
 *          --size=1G \
 *          --numjobs=1 \
 *          --iodepth=1 \
 *          --filename=/zonda2/fio/testfile \
 *          --append=1 \
 *          --master=list://127.0.0.1:28600,127.0.0.1:28601,127.0.0.1:28602 \
 *          --cluster=test_cluster_1 \
 *          --fence_dir=/zonda2/fio
 *
 * 2. Random Read Test (随机读测试)
 *    Performs random read operations on an existing file:
 *
 *    ./fio --name=randread_test \
 *          --ioengine=zondafs \
 *          --rw=randread \
 *          --bs=4k \
 *          --size=1G \
 *          --numjobs=1 \
 *          --filename=/zonda2/fio/testfile \
 *          --master=list://127.0.0.1:28600,127.0.0.1:28601,127.0.0.1:28602 \
 *          --cluster=test_cluster_1 \
 *          --fence_dir=/zonda2/fio
 *
 * KEY PARAMETERS:
 *   --master      : Master address list (required)
 *   --cluster     : Cluster ID (required)
 *   --fence_dir   : Fence directory path (required)
 *   --client      : Client ID (default: "fio_client")
 *   --log_path    : Log file path (default: "./logs")
 *   --role        : Client role (default: "fio_role")
 *   --ip          : Host IP for fence (default: "127.0.0.1")
 */

struct zondafsio_data {
	zonda_fs_client_t* client;
	zonda_fs_file_t* file;
};

struct zondafsio_options {
	void *pad;			/* needed because offset can't be 0 for an option defined used offsetof */
    char *master;
    char *cluster;
    char *client;
    char *fence_dir;
    char *log_path;
    char *role;
    char *ip;
};

static struct fio_option options[] = {
    {
        .name	= "master",
        .lname	= "zonda2 fs master addr",
        .type	= FIO_OPT_STR_STORE,
        .off1   = offsetof(struct zondafsio_options, master),
        .def    = "list://127.0.0.1:28600,127.0.0.1:28601,127.0.0.1:28602",
        .help	= "Master addr of the zonda2 fs",
        .category = FIO_OPT_C_ENGINE,
        .group	= FIO_OPT_G_ZONDAFS,
    },
    {
        .name	= "cluster",
        .lname	= "zonda2 cluster id",
        .type	= FIO_OPT_STR_STORE,
        .off1   = offsetof(struct zondafsio_options, cluster),
        .def    = "",
        .help	= "Cluster id of the zonda2 fs",
        .category = FIO_OPT_C_ENGINE,
        .group	= FIO_OPT_G_ZONDAFS,
    },
    {
        .name	= "client",
        .lname	= "zonda2 client id",
        .type	= FIO_OPT_STR_STORE,
        .off1   = offsetof(struct zondafsio_options, client),
        .def    = "fio_client",
        .help	= "Client id of the zonda2 fs",
        .category = FIO_OPT_C_ENGINE,
        .group	= FIO_OPT_G_ZONDAFS,
    },
    {
        .name	= "fence_dir",
        .lname	= "zonda2 read/write fence_dir",
        .type	= FIO_OPT_STR_STORE,
        .off1   = offsetof(struct zondafsio_options, fence_dir),
        .def    = "",
        .help	= "Fence dir id of the zonda2 fs",
        .category = FIO_OPT_C_ENGINE,
        .group	= FIO_OPT_G_ZONDAFS,
    },
    {
        .name	= "log_path",
        .lname	= "zonda2 cpp client log path",
        .type	= FIO_OPT_STR_STORE,
        .off1   = offsetof(struct zondafsio_options, log_path),
        .def    = "./logs",
        .help	= "Log path of the zonda2 fs",
        .category = FIO_OPT_C_ENGINE,
        .group	= FIO_OPT_G_ZONDAFS,
    },
    {
        .name	= "role",
        .lname	= "zonda2 cpp client role",
        .type	= FIO_OPT_STR_STORE,
        .off1   = offsetof(struct zondafsio_options, role),
        .def    = "fio_role",
        .help	= "Role of the zonda2 fs",
        .category = FIO_OPT_C_ENGINE,
        .group	= FIO_OPT_G_ZONDAFS,
    },
    {
        .name	= "ip",
        .lname	= "zonda2 ip used by io fence",
        .type	= FIO_OPT_STR_STORE,
        .off1   = offsetof(struct zondafsio_options, ip),
        .def    = "127.0.0.1",
        .help	= "The host ip of the zonda2 fence",
        .category = FIO_OPT_C_ENGINE,
        .group	= FIO_OPT_G_ZONDAFS,
    },
    {
        .name	= NULL,
    },
};

static enum fio_q_status fio_zondafs_queue(struct thread_data *td,
					  struct io_u *io_u)
{
	struct zondafsio_data *zd = td->io_ops_data;
	zonda_fs_file_t* file = NULL;
	zonda_error_code_t code;
	unsigned long bytes_written = 0, bytes_read = 0;

	if (!zd || !zd->file) {
		log_err("zondafs: io_ops_data or file is NULL\n");
		io_u->error = EINVAL;
		return FIO_Q_COMPLETED;
	}
	file = zd->file;

	if (io_u->ddir == DDIR_READ) {
		code = zonda_fs_file_read_at(file, io_u->xfer_buflen, io_u->offset, io_u->xfer_buf, &bytes_read);
        if(code != 0 && code != 23014) {
          	io_u->error = EIO;
			return FIO_Q_COMPLETED;
        }
        if(bytes_read != io_u->xfer_buflen) {
          	if(code != 23014) {
          		io_u->error = EIO;
          	}
        }
	} else if (io_u->ddir == DDIR_WRITE) {
		code = zonda_fs_file_append(file,  io_u->xfer_buflen, io_u->xfer_buf, &bytes_written);
        if(code != 0 || bytes_written != io_u->xfer_buflen) {
        	io_u->error = EIO;
        }
	} else {
		log_err("zondafs: Invalid I/O Operation: %d\n", io_u->ddir);
		io_u->error = EINVAL;
	}
	if (io_u->error)
		td_verror(td, io_u->error, "xfer");

	return FIO_Q_COMPLETED;
}

int fio_zondafs_open_file(struct thread_data *td, struct fio_file *f)
{
	struct zondafsio_data *zd = td->io_ops_data;

	zonda_fs_file_t* file = NULL;
	zonda_error_code_t code;
	uint32_t flags;

	if (!zd) {
		log_err("zondafs: io_ops_data is NULL, init() may have failed\n");
		return EINVAL;
	}

	if (!zd->client) {
		log_err("zondafs: client is NULL, init() may have failed. "
			"Check if fio_zondafs_init() was called and succeeded.\n");
		return EINVAL;
	}

	flags = ZONDA_FS_OPEN_FLAGS_RDWR | ZONDA_FS_OPEN_FLAGS_CREAT;
	code = zonda_fs_client_open(zd->client, f->file_name, flags, &file);
	if(code != 0) {
		log_err("zondafs: unable to open file %s, code=%d\n", f->file_name, code);
		return code;
	}
	zd->file = file;
	return 0;
}

int fio_zondafs_close_file(struct thread_data *td, struct fio_file *f)
{
	struct zondafsio_data *zd = td->io_ops_data;

	if (zd && zd->file) {
		zonda_fs_file_close(zd->file);
		zonda_fs_file_destroy(zd->file);
		zd->file = NULL;  // 防止重复释放
	}
	return 0;
}

static void fio_zondafs_cleanup(struct thread_data *td)
{
	struct zondafsio_data *zd = td->io_ops_data;
	if (zd) {
		if (zd->file) {
			log_info("zondafs: closing remaining file in cleanup\n");
			zonda_fs_file_close(zd->file);
			zonda_fs_file_destroy(zd->file);
			zd->file = NULL;
		}
		if (zd->client) {
			zonda_fs_client_destroy(zd->client);
			zd->client = NULL;
		}
		free(zd);
		td->io_ops_data = NULL;
	}
}

static int fio_zondafs_setup(struct thread_data *td)
{
	struct zondafsio_data *zd = td->io_ops_data;
	struct fio_file *f;
	int i;
	uint64_t file_size, total_file_size;

	if (!zd) {
		zd = calloc(1, sizeof(*zd));
		if (!zd) {
			log_err("zondafs: unable to allocate io_ops_data\n");
			return ENOMEM;
		}
		td->io_ops_data = zd;
	}

	total_file_size = 0;
	file_size = 0;

	for_each_file(td, f, i) {
		if(!td->o.file_size_low) {
			file_size = floor(td->o.size / td->o.nr_files);
			total_file_size += file_size;
		}
		else if (td->o.file_size_low == td->o.file_size_high)
			file_size = td->o.file_size_low;
		else {
			file_size = get_rand_file_size(td);
		}
		f->real_file_size = file_size;
	}
	return 0;
}

static int fio_zondafs_init(struct thread_data *td)
{
	struct zondafsio_data *zd = td->io_ops_data;
	struct zondafsio_options *option = td->eo;

	zonda_fs_client_t* client = NULL;
	zonda_error_code_t code;

	zonda_fs_conn_config_t config = {
		.master_addr = option->master,
		.cluster_id = option->cluster,
		.client_id = option->client ? option->client : "fio_client",
		.fence_dir = option->fence_dir,
		.log_path = option->log_path ? option->log_path : "./logs",
		.role = option->role ? option->role : "fio_role",
		.ip = option->ip ? option->ip : "127.0.0.1",
	};
	code = zonda_fs_client_new(&config, &client);
    if(code != 0) {
    	log_err("zondafs: unable to new client, code=%d\n", code);
    	return EINVAL;
    }
	zd->client = client;

	code = zonda_fs_client_fence_directory(client, option->fence_dir);
	if(code != 0) {
		log_err("zondafs: unable to fence dir, code=%d\n", code);
		zonda_fs_client_destroy(client);
		zd->client = NULL;
		return EINVAL;
	}
	return 0;
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name = "zondafs",
	.version = FIO_IOOPS_VERSION,
	.flags = FIO_SYNCIO | FIO_DISKLESSIO | FIO_NODISKUTIL,
	.setup = fio_zondafs_setup,
	.init = fio_zondafs_init,
	.queue = fio_zondafs_queue,
	.open_file = fio_zondafs_open_file,
	.close_file = fio_zondafs_close_file,
	.cleanup = fio_zondafs_cleanup,
	.option_struct_size	= sizeof(struct zondafsio_options),
	.options		= options,
};

static void fio_init fio_zondafs_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_zondafs_unregister(void)
{
	unregister_ioengine(&ioengine);
}