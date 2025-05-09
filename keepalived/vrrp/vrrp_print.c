/*
 * Soft:        Vrrpd is an implementation of VRRPv2 as specified in rfc2338.
 *              VRRP is a protocol which elect a master server on a LAN. If the
 *              master fails, a backup server takes over.
 *              The original implementation has been made by jerome etienne.
 *
 * Part:        Print running VRRP state information
 *
 * Author:      John Southworth, <john.southworth@vyatta.com>
 *
 *              This program is distributed in the hope that it will be useful,
 *              but WITHOUT ANY WARRANTY; without even the implied warranty of
 *              MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *              See the GNU General Public License for more details.
 *
 *              This program is free software; you can redistribute it and/or
 *              modify it under the terms of the GNU General Public License
 *              as published by the Free Software Foundation; either version
 *              2 of the License, or (at your option) any later version.
 *
 * Copyright (C) 2012 John Southworth, <john.southworth@vyatta.com>
 * Copyright (C) 2015-2017 Alexandre Cassen, <acassen@gmail.com>
 */

#include "config.h"

#include <errno.h>
#include <inttypes.h>

#include "logger.h"
#include "list_head.h"
#include "global_data.h"

#include "vrrp.h"
#include "vrrp_data.h"
#include "vrrp_print.h"
#include "utils.h"


void
vrrp_print_data(void)
{
	FILE *fp;

	fp = open_dump_file("");

	if (!fp)
		return;

	dump_data_vrrp(fp);

	fclose(fp);
}

void
vrrp_print_stats(bool clear_stats)
{
	FILE *file;
	vrrp_t *vrrp;
	const char *stats_file;

	stats_file = make_tmp_filename("keepalived.stats");

	file = fopen_safe(stats_file, "we");

	if (!file) {
		log_message(LOG_INFO, "Can't open %s (%d: %s)",
				stats_file, errno, strerror(errno));
		FREE_CONST(stats_file);
		return;
	}

	FREE_CONST(stats_file);

	list_for_each_entry(vrrp, &vrrp_data->vrrp, e_list) {
		fprintf(file, "VRRP Instance: %s\n", vrrp->iname);
		fprintf(file, "  Advertisements:\n");
		fprintf(file, "    Received: %" PRIu64 "\n", vrrp->stats->advert_rcvd);
		fprintf(file, "    Sent: %u\n", vrrp->stats->advert_sent);
		fprintf(file, "  Became master: %u\n", vrrp->stats->become_master);
		fprintf(file, "  Released master: %u\n", vrrp->stats->release_master);
		fprintf(file, "  Packet Errors:\n");
		fprintf(file, "    Length: %" PRIu64 "\n", vrrp->stats->packet_len_err);
		fprintf(file, "    TTL: %" PRIu64 "\n", vrrp->stats->ip_ttl_err);
		fprintf(file, "    Invalid Type: %" PRIu64 "\n",
			vrrp->stats->invalid_type_rcvd);
		fprintf(file, "    Advertisement Interval: %" PRIu64 "\n",
			vrrp->stats->advert_interval_err);
		fprintf(file, "    Address List: %" PRIu64 "\n",
			vrrp->stats->addr_list_err);
		fprintf(file, "  Authentication Errors:\n");
		fprintf(file, "    Invalid Type: %u\n",
			vrrp->stats->invalid_authtype);
#ifdef _WITH_VRRP_AUTH_
		fprintf(file, "    Type Mismatch: %u\n",
			vrrp->stats->authtype_mismatch);
		fprintf(file, "    Failure: %u\n",
			vrrp->stats->auth_failure);
#endif
		fprintf(file, "  Priority Zero:\n");
		fprintf(file, "    Received: %" PRIu64 "\n", vrrp->stats->pri_zero_rcvd);
		fprintf(file, "    Sent: %" PRIu64 "\n", vrrp->stats->pri_zero_sent);

		if (clear_stats)
			memset(vrrp->stats, 0, sizeof(*vrrp->stats));
	}
	fclose(file);
}
