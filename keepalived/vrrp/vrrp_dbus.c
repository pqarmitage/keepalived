/*
 * Soft:        Keepalived is a failover program for the LVS project
 *              <www.linuxvirtualserver.org>. It monitor & manipulate
 *              a loadbalanced server pool using multi-layer checks.
 *
 * Part:        DBus server thread for VRRP
 *
 * Author:      Alexandre Cassen, <acassen@linux-vs.org>
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
 * Copyright (C) 2016-2016 Alexandre Cassen, <acassen@gmail.com>
 */

/* See https://git.gnome.org/browse/glib/tree/gio/tests/fdbus-example-server.c
 * and https://developer.gnome.org/gio/stable/GDBusConnection.html#gdbus-server
 * for examples of coding.
 *
 * To test the DBus service run a command like: dbus-send --system --dest=org.keepalived.Vrrp1 --print-reply object interface.method type:argument
 * e.g.
 * dbus-send --system --dest=org.keepalived.Vrrp1 --print-reply /org/keepalived/Vrrp1/Vrrp org.keepalived.Vrrp1.Vrrp.PrintData
 * or
 * dbus-send --system --dest=org.keepalived.Vrrp1 --print-reply /org/keepalived/Vrrp1/Instance/eth0/1/IPv4 org.freedesktop.DBus.Properties.Get string:'org.keepalived.Vrrp1.Instance' string:'State'
 *
 * To monitor signals, run:
 * dbus-monitor --system type='signal'
 *
 * d-feet is a useful program for interfacing with DBus
 * */

#include "config.h"

#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <gio/gio.h>
#include <ctype.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>

#include "vrrp_dbus.h"
#include "vrrp_data.h"
#include "vrrp_if.h"
#include "vrrp_print.h"
#include "main.h"
#include "memory.h"
#include "logger.h"
#include "timer.h"
#include "scheduler.h"
#include "list.h"
#if HAVE_DECL_CLONE_NEWNET
#include "namespaces.h"
#endif

typedef enum dbus_action {
	DBUS_ACTION_NONE,
	DBUS_PRINT_DATA,
	DBUS_PRINT_STATS,
	DBUS_RELOAD,
	DBUS_CREATE_INSTANCE,
	DBUS_DESTROY_INSTANCE,
	DBUS_SEND_GARP,
	DBUS_GET_NAME,
	DBUS_GET_STATUS,
} dbus_action_t;

typedef enum dbus_error {
	DBUS_SUCCESS,
	DBUS_INTERFACE_NOT_FOUND,
	DBUS_OBJECT_ALREADY_EXISTS,
} dbus_error_t;

typedef struct dbus_queue_ent {
	dbus_action_t action;
	dbus_error_t reply;
	char str[IFNAMSIZ+1];
	int val;
	GVariant *args;
} dbus_queue_ent_t;

/* Global file variables */
static GDBusNodeInfo *vrrp_introspection_data = NULL;
static GDBusNodeInfo *vrrp_instance_introspection_data = NULL;
static GDBusConnection *global_connection;
static GHashTable *objects;
static GMainLoop *loop;

/* Queues between main vrrp thread and dbus thread */
static list dbus_in_queue, dbus_out_queue;
static pthread_mutex_t in_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t out_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static int dbus_in_pipe[2], dbus_out_pipe[2];
static sem_t thread_end;

/* The only characters that are valid in a dbus path are A-Z, a-z, 0-9, _ */
static char *
set_valid_path(char *valid_path, const char *path)
{
	const char *str_in;
	char *str_out;

	for (str_in = path, str_out = valid_path; *str_in; str_in++, str_out++) {
		if (!isalnum(*str_in))
			*str_out = '_';
		else
			*str_out = *str_in;
	}
	*str_out = '\0';

	return valid_path;
}

static bool
valid_path_cmp(const char *path, const char *valid_path)
{
	for ( ; *path && *valid_path; path++, valid_path++) {
		if (!isalnum(*path)) {
			if (*valid_path != '_')
				return true;
		}
		else if (*path != *valid_path)
			return true;
	}

	return *path != *valid_path;
}

static vrrp_t *
get_vrrp_instance(const char *ifname, int vrid, int family)
{
	element e;
	vrrp_t *vrrp;

	if (LIST_ISEMPTY(vrrp_data->vrrp))
		return NULL;

	for (e = LIST_HEAD(vrrp_data->vrrp); e; ELEMENT_NEXT(e)) {
		vrrp = ELEMENT_DATA(e);

		if (vrrp->vrid == vrid &&
		    vrrp->family == family &&
		    !valid_path_cmp(IF_BASE_IFP(vrrp->ifp)->ifname, ifname))
				return vrrp;
	}

	return NULL;
}

static gboolean
unregister_object(gpointer key, gpointer value, gpointer user_data)
{
	g_hash_table_remove(objects, key);

	return g_dbus_connection_unregister_object(global_connection, GPOINTER_TO_UINT(value));
}

static gchar *
dbus_object_create_path_vrrp(void)
{
	return g_strconcat(DBUS_VRRP_OBJECT_ROOT,
#ifdef HAVE_DECL_CLONE_NEWNET
			  (network_namespace) ? "/" : "", (network_namespace) ? network_namespace : "",
#endif
			  (instance_name) ? "/" : "", (instance_name) ? instance_name : "",

			  "/Vrrp", NULL);
}

static gchar *
dbus_object_create_path_instance(const gchar *interface, int vrid, sa_family_t family)
{
	gchar *object_path;
	char standardized_name[sizeof ((vrrp_t*)NULL)->ifp->ifname];
	gchar *vrid_str = g_strdup_printf("%d", vrid);

	set_valid_path(standardized_name, interface);

	object_path = g_strconcat(DBUS_VRRP_OBJECT_ROOT,
#ifdef HAVE_DECL_CLONE_NEWNET
				  (network_namespace) ? "/" : "", (network_namespace) ? network_namespace : "",
#endif
				  (instance_name) ? "/" : "", (instance_name) ? instance_name : "",

				  "/Instance/",
				  standardized_name, "/", vrid_str,
				  (family == AF_INET) ? "/IPv4" : (family == AF_INET6) ? "/IPv6" : "/None",
				  NULL);

	g_free(vrid_str);
	return object_path;
}

static dbus_queue_ent_t *
process_method_call(dbus_action_t action, GVariant *args, bool return_data)
{
// TODO - don't MALLOC - not thread safe - do we need a queue - just one at a time
	dbus_queue_ent_t *ent = MALLOC(sizeof(dbus_queue_ent_t));
	element e;
	char *param = NULL;
	int val = 0;
	char *msg;
	int ret;

	if (!ent)
		return NULL;

	ent->action = action;
	ent->args = NULL;

	if (args) {
		if (g_variant_is_of_type(args, G_VARIANT_TYPE("(su)")))
			g_variant_get(args, "(su)", &param, &val);
		else if (g_variant_is_of_type(args, G_VARIANT_TYPE("(s)")))
			g_variant_get(args, "(s)", &param);
		else if (g_variant_is_of_type(args, G_VARIANT_TYPE("(suu)"))) {
			int family;
			g_variant_get(args, "(suu)", &param, &val, &family);
			ent->args = g_variant_new("(u)", family);
		}
		else if (g_variant_is_of_type(args, G_VARIANT_TYPE("(ssuu)"))) {
			char *iname;
			int family;
			g_variant_get(args, "(ssuu)", &iname, &param, &val, &family);
			ent->args = g_variant_new("(su)", iname, family);
		}
	}

	if (param)
		strcpy(ent->str, param);
	ent->val = val;
	pthread_mutex_lock(&in_queue_lock);
	list_add(dbus_in_queue, ent);
	pthread_mutex_unlock(&in_queue_lock);

	/* Tell the main thread that a queue entry is waiting. Any data works */
	write(dbus_in_pipe[1], ent, 1);

	/* Wait for a response */
	while ((ret = read(dbus_out_pipe[0], ent, 1)) == -1 && errno == EINTR) {
		log_message(LOG_INFO, "dbus_out_pipe read returned EINTR");
	}
	if (ret == -1)
		log_message(LOG_INFO, "DBus response read error - errno = %d", errno);

	/* Free any variant we created */
	if (ent->args)
		g_variant_unref(ent->args);

	/* We could loop through looking for e->data == ent */
	msg = NULL;
	pthread_mutex_lock(&out_queue_lock);
	if (!LIST_ISEMPTY(dbus_out_queue)) {
		e = LIST_HEAD(dbus_out_queue);
		free_list_element(dbus_out_queue, e);
		if (ent != e->data)
			msg = "Returned dbus entry mismatch";
	}
	else
		msg = "Empty dbus out queue";
	pthread_mutex_unlock(&out_queue_lock);
	
	if (msg)
		log_message(LOG_INFO, "%s", msg);

	if (ent->action != action)
		log_message(LOG_INFO, "DBus expected receive action %d and received %d", action, ent->action);
	if (ent->reply != DBUS_SUCCESS) {
		if (ent->reply == DBUS_INTERFACE_NOT_FOUND)
			log_message(LOG_INFO, "Unable to find DBus requested interface %s/%d", param, val);
		else if (ent->reply == DBUS_OBJECT_ALREADY_EXISTS)
			log_message(LOG_INFO, "Unable to create DBus requested object with interface %s/%d", param, val);
		else
			log_message(LOG_INFO, "Unknown DBus reply %d", ent->reply);
	}

	if (!return_data) {
		FREE(ent);
		ent = NULL;
	}

	return ent;
}

/* handles reply to org.freedesktop.DBus.Properties.Get method on any object*/
static GVariant *
handle_get_property(GDBusConnection  *connection,
		    const gchar      *sender,
		    const gchar      *object_path,
		    const gchar      *interface_name,
		    const gchar      *property_name,
		    GError          **error,
		    gpointer          user_data)
{
	GVariant *ret = NULL;
	dbus_queue_ent_t *ent;
	int path_length = DBUS_VRRP_INSTANCE_PATH_DEFAULT_LENGTH;
	gchar **dirs;
	gchar *interface;
	unsigned vrid;
	unsigned family;
	int action;
	GVariant *args;

	if (g_strcmp0(interface_name, DBUS_VRRP_INSTANCE_INTERFACE)) {
		log_message(LOG_INFO, "Interface %s has not been implemented yet", interface_name);
		return NULL;
	}

	if (!g_strcmp0(property_name, "Name"))
		action = DBUS_GET_NAME;
	else if (!g_strcmp0(property_name, "State"))
		action = DBUS_GET_STATUS;
	else {
		log_message(LOG_INFO, "Property %s does not exist", property_name);
		return NULL;
	}

#ifdef HAVE_DECL_CLONE_NEWNET
	if(network_namespace)
		path_length++;
#endif
	if(instance_name)
		path_length++;

	/* object_path will have interface and group as 
	 * the third to last and the second to last levels */
	dirs = g_strsplit(object_path, "/", path_length);
	interface = dirs[path_length-3];
	vrid = atoi(dirs[path_length-2]);
	family = !g_strcmp0(dirs[path_length-1], "IPv4") ? AF_INET : !g_strcmp0(dirs[path_length-1], "IPv6") ? AF_INET6 : AF_UNSPEC;

	args = g_variant_new("(suu)", interface, vrid, family);

	/* We are finished with all the object_path strings now */
	g_strfreev(dirs);

	ent = process_method_call(action, args, true);
	g_variant_unref(args);
	if (ent) {
		if (ent->reply == DBUS_SUCCESS) {
			if (action == DBUS_GET_NAME)
				ret = g_variant_new("(s)", ent->str);
			else if (action == DBUS_GET_STATUS)
				ret = g_variant_new("(u)", ent->val);
		}

		FREE(ent);
	}

	return ret;
}

/* handles method_calls on any object */
static void
handle_method_call(GDBusConnection *connection,
		   const gchar           *sender,
		   const gchar           *object_path,
		   const gchar           *interface_name,
		   const gchar           *method_name,
		   GVariant              *parameters,
		   GDBusMethodInvocation *invocation,
		   gpointer               user_data)
{
	if (!g_strcmp0(interface_name, DBUS_VRRP_INTERFACE)) {
		if (!g_strcmp0(method_name, "PrintData")) {
			process_method_call(DBUS_PRINT_DATA, NULL, false);
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else if (g_strcmp0(method_name, "PrintStats") == 0) {
			process_method_call(DBUS_PRINT_STATS, NULL, false);
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else if (g_strcmp0(method_name, "ReloadConfig") == 0) {
			g_dbus_method_invocation_return_value(invocation, NULL);
			kill(getppid(), SIGHUP);
		} else if (g_strcmp0(method_name, "CreateInstance") == 0) {
			process_method_call(DBUS_CREATE_INSTANCE, parameters, false);
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else if (g_strcmp0(method_name, "DestroyInstance") == 0) {
			process_method_call(DBUS_DESTROY_INSTANCE, parameters, false);
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else {
			log_message(LOG_INFO, "Method %s has not been implemented yet", method_name);
			g_dbus_method_invocation_return_error(invocation, G_DBUS_ERROR, G_DBUS_ERROR_MATCH_RULE_NOT_FOUND, "Method not implemented");
		}

		return;
	}

	if (!g_strcmp0(interface_name, DBUS_VRRP_INSTANCE_INTERFACE)) {
		if (!g_strcmp0(method_name, "SendGarp")) {
			GVariant *name_call =  handle_get_property(connection, sender, object_path,
								   interface_name, "Name", NULL, NULL);
			if (!name_call) {
				log_message(LOG_INFO, "Name property not found");
				return;
			}
			process_method_call(DBUS_SEND_GARP, name_call, false);
			g_dbus_method_invocation_return_value(invocation, NULL);
		} else {
			log_message(LOG_INFO, "Method %s has not been implemented yet", method_name);
			g_dbus_method_invocation_return_error(invocation, G_DBUS_ERROR, G_DBUS_ERROR_MATCH_RULE_NOT_FOUND, "Method not implemented");
		}

		return;
	}

	log_message(LOG_INFO, "Interface %s has not been implemented yet", interface_name);
	g_dbus_method_invocation_return_error(invocation, G_DBUS_ERROR, G_DBUS_ERROR_MATCH_RULE_NOT_FOUND, "Interface not implemented");
}

static const GDBusInterfaceVTable interface_vtable =
{
	handle_method_call,
	handle_get_property,
	NULL /* handle_set_property is null because we have no writeable property */
};

/* first function to be run when trying to own bus,
 * exports objects to the bus */
static void
on_bus_acquired(GDBusConnection *connection,
		const gchar     *name,
		gpointer         user_data)
{
	global_connection = connection;
	gchar *path;
	element e;
	guint instance;
	GError *local_error = NULL;

	log_message(LOG_INFO, "Acquired DBus bus %s\n", name);

	/* register VRRP object */
	path = dbus_object_create_path_vrrp();
	guint vrrp = g_dbus_connection_register_object(connection, path,
							 vrrp_introspection_data->interfaces[0],
							 &interface_vtable, NULL, NULL, NULL);
	g_hash_table_insert(objects, "__Vrrp__", GUINT_TO_POINTER(vrrp));
	g_free(path);
	
	/* for each available VRRP instance, register an object */
	if (LIST_ISEMPTY(vrrp_data->vrrp))
		return;

	for (e = LIST_HEAD(vrrp_data->vrrp); e; ELEMENT_NEXT(e)) {
		vrrp_t * vrrp = ELEMENT_DATA(e);

		path = dbus_object_create_path_instance(IF_NAME(IF_BASE_IFP(vrrp->ifp)), vrrp->vrid, vrrp->family);
		instance = g_dbus_connection_register_object(connection, path,
							     vrrp_instance_introspection_data->interfaces[0],
							     &interface_vtable, NULL, NULL, NULL);
		g_free(path);

		if (instance)
			g_hash_table_insert(objects, vrrp->iname, GUINT_TO_POINTER(instance));
	}

	/* Send a signal to say we have started */
	path = dbus_object_create_path_vrrp();
	g_dbus_connection_emit_signal(global_connection, NULL, path,
				      DBUS_VRRP_INTERFACE, "VrrpStarted", NULL, &local_error);
	g_free(path);

	/* Notify DBus of the state of our instances */
	for (e = LIST_HEAD(vrrp_data->vrrp); e; ELEMENT_NEXT(e)) {
		vrrp_t * vrrp = ELEMENT_DATA(e);
		dbus_send_state_signal(vrrp);
	}
}

/* run if bus name is acquired successfully */
static void
on_name_acquired(GDBusConnection *connection,
		 const gchar     *name,
		 gpointer         user_data)
{
	log_message(LOG_INFO, "Acquired the name %s on the session bus\n", name);
}

/* run if bus name or connection are lost */
static void
on_name_lost(GDBusConnection *connection,
	     const gchar     *name,
	     gpointer         user_data)
{
	log_message(LOG_INFO, "Lost the name %s on the session bus\n", name);
	global_connection = connection;
	g_hash_table_foreach_remove(objects, unregister_object, NULL);
	objects = NULL;
	global_connection = NULL;
}

static gchar*
read_file(gchar* filepath)
{
	FILE * f;
	long length;
	gchar *ret = NULL;

	f = fopen(filepath, "rb");
	if (f) {
		fseek(f, 0, SEEK_END);
		length = ftell(f);
		fseek(f, 0, SEEK_SET);

		/* We can't use MALLOC since it isn't thread safe */
		ret = malloc(length + 1);
		if (ret) {
			fread(ret, 1, length, f);
			ret[length] = '\0';
		}
		else
			log_message(LOG_INFO, "Unable to read Dbus file %s", filepath);

		fclose(f);
	}
	return ret;
}

static void *
dbus_main(__attribute__ ((unused)) void *unused)
{
	gchar *introspection_xml;
	guint owner_id;

	objects = g_hash_table_new(g_str_hash, g_str_equal);

	/* DBus service org.keepalived.Vrrp1 exposes two interfaces, Vrrp and Instance.
	 * Vrrp is implemented by a single VRRP object for general purposes, such as printing
	 * data or signaling that the VRRP process has been stopped.
	 * Instance is implemented by an Instance object for every VRRP Instance in vrrp_data.
	 * It exposes instance specific methods and properties.
	 */
#ifdef DBUS_NEED_G_TYPE_INIT
	g_type_init();
#endif
	GError *error;

	/* read service interface data from xml files */
	introspection_xml = read_file(DBUS_VRRP_INTERFACE_FILE_PATH);
	if (!introspection_xml)
		return NULL;
	vrrp_introspection_data = g_dbus_node_info_new_for_xml(introspection_xml, &error);
	free(introspection_xml);
	if (!vrrp_introspection_data) {
		log_message(LOG_INFO, "%s", error->message);
		return NULL;
	}

	introspection_xml = read_file(DBUS_VRRP_INSTANCE_INTERFACE_FILE_PATH);
	if (!introspection_xml)
		return NULL;
	vrrp_instance_introspection_data = g_dbus_node_info_new_for_xml(introspection_xml, &error);
	free(introspection_xml);
	if (!vrrp_instance_introspection_data) {
		log_message(LOG_INFO, "%s", error->message);
		return NULL;
	}

	owner_id = g_bus_own_name(G_BUS_TYPE_SYSTEM,
				  DBUS_SERVICE_NAME,
				  G_BUS_NAME_OWNER_FLAGS_NONE,
				  on_bus_acquired,
				  on_name_acquired,
				  on_name_lost,
				  NULL,  /* user_data */
				  NULL); /* user_data_free_func */

	loop = g_main_loop_new(NULL, FALSE);
	g_main_loop_run(loop);

	/* cleanup after loop terminates */
	g_main_loop_unref(loop);
	g_bus_unown_name(owner_id);
	global_connection = NULL;

	sem_post(&thread_end);
	pthread_exit(0);
}

/* The following functions are run in the context of the main vrrp thread */

/* send signal VrrpStatusChange
 * containing the new state of vrrp */
void
dbus_send_state_signal(vrrp_t *vrrp)
{
	GError *local_error = NULL;

	/* the interface will go through the initial state changes before
	 * the main loop can be started and global_connection initialised */
	if (global_connection == NULL)
		return;

	gchar *object_path = dbus_object_create_path_instance(IF_NAME(IF_BASE_IFP(vrrp->ifp)), vrrp->vrid, vrrp->family);

	GVariant *args = g_variant_new("(u)", vrrp->state);
	g_dbus_connection_emit_signal(global_connection, NULL, object_path,
				      DBUS_VRRP_INSTANCE_INTERFACE,
				      "VrrpStatusChange", args, &local_error);

	g_free(object_path);
}

/* send signal VrrpRestarted */
void
dbus_send_restart_signal(void)
{
	GError *local_error = NULL;
	gchar *path;

	if (global_connection == NULL)
		return;

	path = dbus_object_create_path_vrrp();
	g_dbus_connection_emit_signal(global_connection, NULL, path,
				      DBUS_VRRP_INTERFACE, "VrrpReloaded", NULL, &local_error);
	g_free(path);
}

static int
dbus_create_object_params(char *instance_name, const char *interface_name, int vrid, int family) 
{
	gchar *object_path;

	if (g_hash_table_lookup(objects, instance_name)) {
		log_message(LOG_INFO, "An object for instance %s already exists", instance_name);
		return DBUS_OBJECT_ALREADY_EXISTS;	
	}

	object_path = dbus_object_create_path_instance(interface_name, vrid, family);

	guint instance = g_dbus_connection_register_object(global_connection, object_path,
						vrrp_instance_introspection_data->interfaces[0],
						&interface_vtable, NULL, NULL, NULL);

	if (instance) {
		g_hash_table_insert(objects, instance_name, GUINT_TO_POINTER(instance));
		log_message(LOG_INFO, "Added DBus object for instance %s on path %s", instance_name, object_path);
	}
	g_free(object_path);

	return DBUS_SUCCESS;
}

static void
dbus_create_object(vrrp_t *vrrp)
{
	dbus_create_object_params(vrrp->iname, IF_NAME(IF_BASE_IFP(vrrp->ifp)),vrrp->vrid, vrrp->family);
}

static void
dbus_unregister_object(char *str)
{
	gpointer value = g_hash_table_lookup(objects, str);
	if (value) {
		unregister_object(str, value, NULL);
		log_message(LOG_INFO, "Deleted DBus object for instance %s", str);
	} else
		log_message(LOG_INFO, "DBus object not found for instance %s", str);
}

void
dbus_remove_object(vrrp_t *vrrp)
{
	dbus_unregister_object(vrrp->iname);
}

void
dbus_add_new_instances(list o, list n)
{
	element e1, e2, e3;
	vrrp_t *vrrp_n, *vrrp_o, *vrrp_n3;

	if (LIST_ISEMPTY(n))
		return;

	for (e1 = LIST_HEAD(n); e1; ELEMENT_NEXT(e1)) {
		char *n_name;
		bool match_found;

		if (LIST_ISEMPTY(o)) {
			dbus_create_object(vrrp_n);
			continue;
		}

		vrrp_n = ELEMENT_DATA(e1);
		n_name = IF_BASE_IFP(vrrp_n->ifp)->ifname;

		/* Try an find an instance with same vrid/family/interface that existed before and now */
		for (e2 = LIST_HEAD(o), match_found = false; e2 && !match_found; ELEMENT_NEXT(e2)) {
			vrrp_o = ELEMENT_DATA(e2);

			if (vrrp_n->vrid == vrrp_o->vrid &&
			    vrrp_n->family == vrrp_o->family &&
			    !strcmp(n_name, IF_BASE_IFP(vrrp_o->ifp)->ifname)) {
				/* If the old instance exists in the new config,
				 * then the dbus object will exist */
				if (!strcmp(vrrp_n->iname, vrrp_o->iname)) {
					match_found = true;
					break;
				}

				/* Check if the old instance name we found still exists
				 * (but has a different vrid/family/interface) */
				for (e3 = LIST_HEAD(n); e3; ELEMENT_NEXT(e3)) {
					vrrp_n3 = ELEMENT_DATA(e3);
					if (!strcmp(vrrp_o->iname, vrrp_n3->iname)) {
						match_found = true;
						break;
					}
				}
			}
		}

		if (match_found)
			continue;

		dbus_create_object(vrrp_n);
	}
}

static void
return_dbus_msg(dbus_queue_ent_t *ent)
{
	pthread_mutex_lock(&out_queue_lock);
	list_add(dbus_out_queue, ent);
	pthread_mutex_unlock(&out_queue_lock);

	write(dbus_out_pipe[1], ent, 1);
}

static dbus_queue_ent_t *
get_queue_ent(void)
{
	dbus_queue_ent_t *ent = NULL;
	element e;

	pthread_mutex_lock(&in_queue_lock);
	if (!LIST_ISEMPTY(dbus_in_queue)) {
		e = LIST_HEAD(dbus_in_queue);
		free_list_element(dbus_in_queue, e);
		ent = e->data;
	}
	pthread_mutex_unlock(&in_queue_lock);

	return ent;
}

static int
handle_dbus_msg(thread_t *thread)
{
	dbus_queue_ent_t *ent;
	char recv_buf;
	list l;
	element e;
	vrrp_t *vrrp;
	unsigned family;

	read(dbus_in_pipe[0], &recv_buf, 1);

	if ((ent = get_queue_ent()) != NULL) {
		ent->reply = DBUS_SUCCESS;

		if (ent->action == DBUS_PRINT_DATA) {
			log_message(LOG_INFO, "Printing VRRP data on DBus request");
			vrrp_print_data();

		}
		else if (ent->action == DBUS_PRINT_STATS) {
			log_message(LOG_INFO, "Printing VRRP stats on DBus request");
			vrrp_print_stats();
		}
		else if (ent->action == DBUS_CREATE_INSTANCE) {
			gchar *name;
			g_variant_get(ent->args, "(su)", &name, &family);

			ent->reply = dbus_create_object_params(name, ent->str, ent->val, family == 4 ? AF_INET : family == 6 ? AF_INET6 : AF_UNSPEC);
		}
		else if (ent->action == DBUS_DESTROY_INSTANCE) {
			dbus_unregister_object(ent->str);
		}
		else if (ent->action == DBUS_SEND_GARP) {
			ent->reply = DBUS_INTERFACE_NOT_FOUND;
			l = vrrp_data->vrrp;
			if (!LIST_ISEMPTY(l)) {
				for (e = LIST_HEAD(l); e; ELEMENT_NEXT(e)) {
					vrrp = ELEMENT_DATA(e);
					if (!strcmp(vrrp->iname, ent->str)) {
						log_message(LOG_INFO, "Sending garps on %s on DBus request", vrrp->iname);
						vrrp_send_link_update(vrrp, 1);
						ent->reply = DBUS_SUCCESS;
						break;
					}
				}
			}
		}
		else if (ent->action == DBUS_GET_NAME ||
			 ent->action == DBUS_GET_STATUS) {
			/* we look for the vrrp instance object that corresponds to our interface and group */
			ent->reply = DBUS_INTERFACE_NOT_FOUND;

			g_variant_get(ent->args, "(u)", &family);
			vrrp = get_vrrp_instance(ent->str, ent->val, family);

			if (vrrp) {
				/* the property_name argument is the property we want to Get */
				if (ent->action == DBUS_GET_NAME) {
					strncpy(ent->str, vrrp->iname, sizeof(ent->str));
					ent->str[sizeof(ent->str) - 1] = '\0';
				}
				else if (ent->action == DBUS_GET_STATUS)
					ent->val = vrrp->state;
				else {
					/* How did we get here? */
					ent->val = 0;
					ent->str[0] = '\0';
				}
				ent->reply = DBUS_SUCCESS;
			}
		}
		return_dbus_msg(ent);
	}

	thread_add_read(master, handle_dbus_msg, NULL, dbus_in_pipe[0], TIMER_NEVER);

	return 0;
}

bool
dbus_start(void)
{
	pthread_t dbus_thread;
	sigset_t sigset, cursigset;

	dbus_in_queue = alloc_list(NULL, NULL);
	dbus_out_queue = alloc_list(NULL, NULL);

#ifdef HAVE_PIPE2
	if (pipe2(dbus_in_pipe, O_CLOEXEC)) {
		log_message(LOG_INFO, "Unable to create inbound dbus pipe - disabling DBus");
		return false;
	}
	if (pipe2(dbus_out_pipe, O_CLOEXEC)) {
		log_message(LOG_INFO, "Unable to create outbound dbus pipe - disabling DBus");
		close(dbus_in_pipe[0]);
		close(dbus_in_pipe[1]);
		return false;
	}
#else
	if (pipe(dbus_in_pipe)) {
		log_message(LOG_INFO, "Unable to create inbound dbus pipe - disabling DBus");
		return false;
	}
	if (pipe(dbus_out_pipe)) {
		log_message(LOG_INFO, "Unable to create outbound dbus pipe - disabling DBus");
		close(dbus_in_pipe[0]);
		close(dbus_in_pipe[1]);
		return false;
	}
	fcntl (dbus_in_pipe[0], F_SETFD, FD_CLOEXEC | fcntl(dbus_in_pipe[0], F_GETFD));
	fcntl (dbus_in_pipe[1], F_SETFD, FD_CLOEXEC | fcntl(dbus_in_pipe[1], F_GETFD));
	fcntl (dbus_out_pipe[0], F_SETFD, FD_CLOEXEC | fcntl(dbus_out_pipe[0], F_GETFD));
	fcntl (dbus_out_pipe[1], F_SETFD, FD_CLOEXEC | fcntl(dbus_out_pipe[1], F_GETFD));
#endif

	/* We don't want the main thread to block when using the pipes */
	fcntl(dbus_in_pipe[0], F_SETFL, O_NONBLOCK | fcntl(dbus_in_pipe[0], F_GETFL));
	fcntl(dbus_out_pipe[1], F_SETFL, O_NONBLOCK | fcntl(dbus_out_pipe[1], F_GETFL));

	thread_add_read(master, handle_dbus_msg, NULL, dbus_in_pipe[0], TIMER_NEVER);

	/* Initialise the thread termination semaphore */
	sem_init(&thread_end, 0, 0);

	/* Block signals (all) we don't want the new thread to process */
	sigemptyset(&sigset);
	pthread_sigmask(SIG_SETMASK, &sigset, &cursigset);

	/* Now create the dbus thread */
	pthread_create(&dbus_thread, NULL, &dbus_main, NULL);

	/* Reenable our signals */
	pthread_sigmask(SIG_SETMASK, &cursigset, NULL);

	return true;
}

void
dbus_stop(void)
{
	GError *local_error = NULL;
	struct timespec thread_end_wait;
	int ret;
	gchar *path;

	pthread_mutex_lock(&in_queue_lock);
	free_list(&dbus_in_queue);
	dbus_in_queue = NULL;
	pthread_mutex_unlock(&in_queue_lock);

	pthread_mutex_lock(&out_queue_lock);
	free_list(&dbus_out_queue);
	dbus_out_queue = NULL;
	pthread_mutex_unlock(&out_queue_lock);

	if (global_connection != NULL) {
		path = dbus_object_create_path_vrrp();
		g_dbus_connection_emit_signal(global_connection, NULL, path,
					      DBUS_VRRP_INTERFACE, "VrrpStopped", NULL, &local_error);
		g_free(path);
	}

	g_main_loop_quit(loop);

	g_dbus_node_info_unref(vrrp_introspection_data);
	g_dbus_node_info_unref(vrrp_instance_introspection_data);

	clock_gettime(CLOCK_REALTIME, &thread_end_wait);
	thread_end_wait.tv_sec += 1;
	while ((ret = sem_timedwait(&thread_end, &thread_end_wait)) == -1 && errno == EINTR) ;

	if (ret == -1 ) {
		if (errno == ETIMEDOUT) 
			log_message(LOG_INFO, "DBus thread termination timed out");
		else
			log_message(LOG_INFO, "sem_timewait error %d", errno);
	}
	else {
		log_message(LOG_INFO, "Released DBus");
		sem_destroy(&thread_end);
	}
}
