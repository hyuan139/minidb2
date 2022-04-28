/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>

#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif

int main(int argc, char **argv)
{
	int rc = 0;
	token_list *tok_list = NULL, *tok_ptr = NULL, *tmp_tok_ptr = NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n", tok_ptr->tok_string, tok_ptr->tok_class,
				   tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}

		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					(tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

		/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			tmp_tok_ptr = tok_ptr->next;
			free(tok_ptr);
			tok_ptr = tmp_tok_ptr;
		}
	}

	return rc;
}

/*************************************************************
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char *command, token_list **tok_list)
{
	int rc = 0, i, j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;

	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset((void *)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do
			{
				temp_string[i++] = *cur++;
			} while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
					if (KEYWORD_OFFSET + j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET + j >= F_SUM)
						t_class = function_name;
					else
						t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET + j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
						add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do
			{
				temp_string[i++] = *cur++;
			} while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*') || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
			case '(':
				t_value = S_LEFT_PAREN;
				break;
			case ')':
				t_value = S_RIGHT_PAREN;
				break;
			case ',':
				t_value = S_COMMA;
				break;
			case '*':
				t_value = S_STAR;
				break;
			case '=':
				t_value = S_EQUAL;
				break;
			case '<':
				t_value = S_LESS;
				break;
			case '>':
				t_value = S_GREATER;
				break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
		else if (*cur == '\'')
		{
			/* Find STRING_LITERRAL */
			int t_class;
			cur++;
			do
			{
				temp_string[i++] = *cur++;
			} while ((*cur) && (*cur != '\''));

			temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else /* must be a ' */
			{
				add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
				cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}

	return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list *)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
		((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_DROP) &&
			 ((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
			 ((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
			 ((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_INSERT) &&
			 ((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_SELECT) && (cur->next->tok_value != NULL))
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_DELETE) && ((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
	{
		printf("DELETE statement\n");
		cur_cmd = DELETE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_UPDATE) && (cur->next != NULL))
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch (cur_cmd)
		{
		case CREATE_TABLE:
			rc = sem_create_table(cur);
			break;
		case DROP_TABLE:
			rc = sem_drop_table(cur);
			break;
		case LIST_TABLE:
			rc = sem_list_tables();
			break;
		case LIST_SCHEMA:
			rc = sem_list_schema(cur);
			break;
		case INSERT:
			rc = sem_insert(cur);
			break;
		case SELECT:
			rc = sem_select(cur);
			break;
		case DELETE:
			rc = sem_delete(cur);
			break;
		case UPDATE:
			rc = sem_update(cur);
			break;
		default:; /* no action */
		}
	}

	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry col_entry[MAX_NUM_COL];

	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				// Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for (i = 0; i < cur_id; i++)
						{
							/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string) == 0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false; /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
								/* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;

								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										(cur->tok_value != K_NOT) &&
										(cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										col_entry[cur_id].col_len = sizeof(int);

										if ((cur->tok_value == K_NOT) &&
											(cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}
										else if ((cur->tok_value == K_NOT) &&
												 (cur->next->tok_value == K_NULL))
										{
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}

										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												(cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								} // end of T_INT processing
								else
								{
									// It must be char() or varchar()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;

										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;

											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;

												if ((cur->tok_value != S_COMMA) &&
													(cur->tok_value != K_NOT) &&
													(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														(cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
															 (cur->next->tok_value == K_NULL))
													{
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}

													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) && (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
														{
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										} /* end char(n) processing */
									}
								} /* end char processing */
							}
						} // duplicate column name
					}	  // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));

				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) +
										 sizeof(cd_entry) * tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry *)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void *)new_entry,
							   (void *)&tab_entry,
							   sizeof(tpd_entry));

						memcpy((void *)((char *)new_entry + sizeof(tpd_entry)),
							   (void *)col_entry,
							   sizeof(cd_entry) * tab_entry.num_columns);

						rc = add_tpd_to_list(new_entry);

						free(new_entry);
					}
				}
			}
		}
	}
	// printf("\nPREPARING TO CREATE .tab file\n");
	//  create <table_name>.tab file
	FILE *fhandle = NULL;

	table_file_header *tab_header;
	fhandle = fopen(strcat(tab_entry.table_name, ".tab"), "wbc");
	int record_size = 0; // round to nearest 4 bytes
	int j = 0;
	while (j < cur_id)
	{
		record_size += (1 + col_entry[j].col_len); // Gets record size
		j++;
	}
	// round to nearest 4 bytes
	if (record_size % 4 != 0)
	{
		record_size = record_size + (4 - (record_size % 4));
	}
	// printf("\nRecord size: %d\n", record_size);
	// printf("\nWORKING WITH tab header\n");
	// tab_header = (table_file_header *)calloc(1, sizeof(table_file_header));
	tab_header = (table_file_header *)calloc(1, sizeof(table_file_header)); // -4 for bytes from pointer
	tab_header->record_size = record_size;
	tab_header->num_records = 0;
	tab_header->file_header_flag = 0; // Just set to 0.
	// tab_header->tpd_ptr->tpd_size = 0; // Just set to 0. Apparently just for convenience, don't need to use. // Apparently Causing segmentation fault
	tab_header->record_offset = sizeof(table_file_header); // size of header (minimum 28); -4 bytes for pointer
	tab_header->file_size = sizeof(table_file_header);	   // -4 bytes for the pointer
	// printf("\nSize of tab_file_header: %d\n", sizeof(tab_header->file_size) + sizeof(tab_header->record_size) + +sizeof(tab_header->num_records) + sizeof(tab_header->record_offset) + sizeof(tab_header->file_header_flag) + sizeof(tab_header->tpd_ptr));
	//  printf("\nEND WORKING WITH tab header\n");
	// fwrite(tab_header, sizeof(tab_header), 6, fhandle);
	// fwrite(tab_header, sizeof(table_file_header) - 4, 1, fhandle);
	fwrite(tab_header, sizeof(table_file_header), 1, fhandle);
	fflush(fhandle);
	fclose(fhandle);
	// printf("\nFINISHED TO CREATE .tab file\n");
	return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		(cur->tok_class != identifier) &&
		(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}

	// Drop .tab file
	if (remove(strcat(cur->tok_string, ".tab")) != 0)
	{
		printf("\n%s.tab file failed to delete\n", cur->tok_string);
	}
	return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry *)((char *)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

	return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN + 1];
	char filename[MAX_IDENT_LEN + 1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			(cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN + 1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;

					if ((cur->tok_class != keyword) &&
						(cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if ((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags);

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags);
						}

						/* Next, write the cd_entry information */
						for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset);
							 i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}

						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error
				}	  // Table not exist
			}		  // no semantic errors
		}			  // Invalid table name
	}				  // Invalid statement

	return rc;
}

int sem_insert(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	cur = t_list;
	table_file_header *old_header = (table_file_header *)calloc(1, sizeof(table_file_header));
	FILE *fhandle = NULL;
	char filename[MAX_IDENT_LEN + 4];
	char tablename[MAX_IDENT_LEN + 1];
	strcpy(tablename, cur->tok_string);
	strcpy(filename, strcat(cur->tok_string, ".tab"));
	if ((fhandle = fopen(filename, "rbc")) == NULL)
	{
		printf("Error while opening %s file\n", filename);
		rc = FILE_OPEN_ERROR;
		cur->tok_value = INVALID;
	}
	else
	{
		// Read the old header information from the tab file.
		fread(old_header, sizeof(table_file_header), 1, fhandle);
		old_header->tpd_ptr = get_tpd_from_list(tablename);
		fclose(fhandle);
		rc = add_row_to_file(old_header, cur->next);
		if (rc == 0)
		{
			printf("row inserted successfully.\n");
			free(old_header);
		}
		else
		{
			printf("Failure to insert.\n");
		}
	}

	return rc;
}

int add_row_to_file(table_file_header *old_head, token_list *t_list)
{
	int rc = 0;
	FILE *fhandle = NULL;
	token_list *cur = t_list;
	table_file_header *old_header = old_head;
	table_file_header *new_header = NULL;
	cd_entry *col_entry = NULL;
	tpd_entry *tab_entry = old_head->tpd_ptr;
	char filename[MAX_IDENT_LEN + 4];
	char tablename[MAX_IDENT_LEN + 1];
	char *recordBuffer;
	bool column_done = false;
	int colNum = 0;
	int oldFileSize = 0;
	char column_values[MAX_NUM_COL][1024];
	char **record_ptr;
	char *old_file;
	oldFileSize = old_header->file_size;
	old_file = (char *)calloc(1, oldFileSize);
	bool colNull[MAX_NUM_COL];
	// Example: insert into students values(3,'John','Doe', 9)
	if (cur->tok_class != keyword)
	{
		// error; current token is not "Values"
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;
		if (cur->tok_value != S_LEFT_PAREN)
		{
			// error
			rc = INVALID_INSERT_DEFINITION;
			cur->tok_value = INVALID;
		}
		else
		{
			// memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));
			cur = cur->next; // get token after left paren
			// loop through tokens for columns
			int j = 0;
			do
			{
				// should be a value for column field
				if ((cur->tok_value != INT_LITERAL) && (cur->tok_value != STRING_LITERAL) && (cur->tok_value != K_NULL))
				{
					rc = INVALID_INSERT_DEFINITION;
					cur->tok_value = INVALID;
				}
				else
				{
					if (cur->tok_value == K_NULL)
					{
						colNull[j] = true;
					}
					else
					{
						strcpy(column_values[j], cur->tok_string);
						colNull[j] = false;
					}
					cur = cur->next;
					// token must either be comma or right paren
					if (!rc)
					{
						if ((cur->tok_value != S_COMMA) && (cur->tok_value != S_RIGHT_PAREN))
						{
							rc = INVALID_INSERT_DEFINITION;
							cur->tok_value = INVALID;
						}
						else
						{
							if (cur->tok_value == S_RIGHT_PAREN)
							{
								column_done = true;
							}
							// consume the comma
							cur = cur->next;
						}
					}
					j++;
				}
			} while ((rc == 0) && !column_done);
		}
	}
	if (!rc)
	{
		strcpy(tablename, tab_entry->table_name);
		strcpy(filename, strcat(tablename, ".tab"));
		new_header = (table_file_header *)calloc(1, sizeof(table_file_header) + (old_header->record_size * (1 * (old_header->num_records + 1))));
		if ((fhandle = fopen(filename, "rbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
			printf("\nFailed to open file\n");
		}
		else
		{
			// read previous file content
			fread((void *)((char *)old_file), oldFileSize, 1, fhandle);
			memcpy((void *)((char *)new_header), (void *)old_file, oldFileSize); // copy old file
			if ((fhandle = fopen(filename, "wbc")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				new_header->num_records += 1;
				new_header->file_size = oldFileSize + old_header->record_size;
				new_header->tpd_ptr = 0;
				recordBuffer = (char *)calloc(1, old_header->record_size);
				memset((void *)recordBuffer, '\0', old_header->record_size);
				int i = 0;
				int k = 0;
				int value_num = 0;
				int int_size = sizeof(int);
				int string_size = 0;
				for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
				{
					// add column value to record buffer
					if ((col_entry->col_type == T_INT) && !(colNull[i]))
					{

						memcpy((void *)((char *)recordBuffer + k), (void *)((char *)&int_size), 1);
						// recordBuffer[k];
						k++; // take 1 byte

						value_num = atoi(column_values[i]);
						memcpy((void *)((char *)recordBuffer + k), (void *)((char *)&value_num), sizeof(int));
						// recordBuffer[k];
						k += sizeof(int);
					}
					else if (((col_entry->col_type == T_CHAR) || (col_entry->col_type == T_VARCHAR)) && !(colNull[i]))
					{
						string_size = strlen(column_values[i]);
						memcpy((void *)((char *)recordBuffer + k), (void *)((char *)&string_size), 1);
						// recordBuffer[k];
						k++;
						memcpy((void *)((char *)recordBuffer + k), (void *)column_values[i], strlen(column_values[i]));
						k += col_entry->col_len;
					}
					else if ((col_entry->col_type == T_INT) && (colNull[i]))
					{
						memcpy((void *)((char *)recordBuffer + k), (void *)((char *)&int_size), 1);
						// recordBuffer[k];
						k++; // take 1 byte
						// recordBuffer[k];
						k += sizeof(int);
					}
					else if (((col_entry->col_type == T_CHAR) || (col_entry->col_type == T_VARCHAR)) && (colNull[i]))
					{
						string_size = 0; // null value length
						memcpy((void *)((char *)recordBuffer + k), (void *)((char *)&string_size), 1);
						// recordBuffer[k];
						k++;
						// recordBuffer[k];
						k += col_entry->col_len;
					}
				}
				memcpy((void *)((char *)new_header + new_header->record_offset + (new_header->record_size * old_header->num_records)), (void *)((char *)recordBuffer), new_header->record_size);
				fwrite(new_header, sizeof(table_file_header) + (old_header->record_size * new_header->num_records), 1, fhandle);
			}
		}
	}
	return rc;
}

int sem_select(token_list *t_list)
{
	int rc = 0;
	FILE *fhandle = NULL;
	FILE *fhandle2 = NULL;
	token_list *cur = t_list;
	tpd_entry *tab_entry = NULL;
	tpd_entry *tab2_entry = NULL;
	cd_entry *col_entry = NULL;
	cd_entry *col_entry2 = NULL;
	table_file_header *old_header = NULL;
	table_file_header *old_header2 = NULL;
	struct stat file_stat;
	struct stat file2_stat;
	char filename[MAX_IDENT_LEN + 4];
	char filename2[MAX_IDENT_LEN + 4];
	char tablename[MAX_IDENT_LEN + 1];
	char table2name[MAX_IDENT_LEN + 1];
	char column_names[MAX_NUM_COL][1024];
	char column_length[MAX_NUM_COL][1024];
	char column_type[MAX_NUM_COL][1024];
	char column_names2[MAX_NUM_COL][1024];
	char column_length2[MAX_NUM_COL][1024];
	char column_type2[MAX_NUM_COL][1024];
	int i;
	int k;
	int m, n;

	// check correct select syntax
	if ((cur->tok_value) != S_STAR)
	{
		rc = INVALID_SELECT_DEFINITION;
		cur->tok_class = error;
		cur->tok_value = INVALID;
	}
	// star symbol -> next token
	cur = cur->next;
	if ((cur->tok_value) != K_FROM)
	{
		rc = INVALID_SELECT_DEFINITION;
		cur->tok_class = error;
		cur->tok_value = INVALID;
	}
	if (!rc)
	{
		// from keyword -> next token
		cur = cur->next; // table name
		if (!(strcmp(cur->next->tok_string, "")))
		{
			strcpy(tablename, cur->tok_string);
			strcpy(filename, strcat(cur->tok_string, ".tab"));

			tab_entry = get_tpd_from_list(tablename);
			for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
			{
				// store column name and column length in two arrays to retrieve value later for format
				strcpy(column_names[i], col_entry->col_name);
				// if (col_entry->col_len <= 4)
				//{
				//	sprintf(column_length[i], "%d", col_entry->col_len + 10);
				//	}
				//	else
				//	{
				sprintf(column_length[i], "%d", col_entry->col_len);
				//}
				sprintf(column_type[i], "%d", col_entry->col_type);
			}
			if ((fhandle = fopen(filename, "rbc")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				fstat(fileno(fhandle), &file_stat);
				old_header = (table_file_header *)calloc(1, file_stat.st_size);
				fread((void *)((char *)old_header), file_stat.st_size, 1, fhandle);
				// print table divider
				print_separator(old_header->record_size);
				int j = 0;
				int length = 0; // length for the table divider
				// TODO: Fix Formatting later
				while (j < tab_entry->num_columns)
				{
					if ((j == tab_entry->num_columns - 1) && (atoi(column_type[j]) == T_INT))
					{
						printf("%*s", atoi(column_length[j]), column_names[j]);
					}
					else if ((j == tab_entry->num_columns - 1) && ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR)))
					{
						printf("%-*s", atoi(column_length[j]), column_names[j]);
					}
					else if ((atoi(column_type[j]) == T_INT))
					{
						printf("%*s | ", atoi(column_length[j]), column_names[j]);
					}
					else if (((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR)))
					{
						printf("%-*s | ", atoi(column_length[j]), column_names[j]);
					}
					// length += atoi(column_length[j]);
					j++;
				}
				printf("\n");
				print_separator(old_header->record_size);
				char *record = NULL;
				char *value;
				j = 0;
				int recordOffset = 0;
				for (i = 0; i < old_header->num_records; i++)
				{
					record = (char *)calloc(1, old_header->record_size);
					memcpy((void *)((char *)record), (void *)((char *)old_header + old_header->record_offset + (i * old_header->record_size)), old_header->record_size);
					while (j < tab_entry->num_columns)
					{
						value = NULL;
						if (atoi(column_type[j]) == T_INT)
						{
							// process as a int
							value = (char *)calloc(1, sizeof(int));
							recordOffset += 1; // account for length byte
							memcpy((void *)((char *)value), (void *)((char *)record + recordOffset), sizeof(int));
							recordOffset += sizeof(int);
							char hexValue[16];
							char temp[16];
							memset(hexValue, '\0', 16);
							strcat(hexValue, "0x");
							sprintf(temp, "%x", (int)value[1]);
							strcat(hexValue, temp);
							sprintf(temp, "%x", (int)value[0]);
							strcat(hexValue, temp);
							long decimal = strtol(hexValue, NULL, 16);
							if ((j == tab_entry->num_columns - 1) && (decimal != 0))
							{
								printf("%*ld", atoi(column_length[j]), decimal);
							}
							else if ((j == tab_entry->num_columns - 1) && (decimal == 0))
							{
								printf("%*s", atoi(column_length[j]), " -");
							}
							else if ((decimal != 0))
							{
								printf("%*ld | ", atoi(column_length[j]), decimal);
							}
							else if ((decimal == 0))
							{
								printf("%*s | ", atoi(column_length[j]), " -");
							}
						}
						else if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
						{
							// process as string
							value = (char *)calloc(1, atoi(column_length[j]));
							recordOffset += 1; // account for btye for length of the value
							memcpy((void *)((char *)value), (void *)((char *)record + recordOffset), atoi(column_length[j]));
							recordOffset += atoi(column_length[j]);
							if ((j == tab_entry->num_columns - 1) && (strlen(value) != 0))
							{
								printf("%-*s", atoi(column_length[j]), value);
							}
							else if ((j == tab_entry->num_columns - 1) && (strlen(value) == 0))
							{
								printf("%-*s", atoi(column_length[j]), " -");
							}
							else if ((strlen(value) != 0))
							{
								printf("%-*s | ", atoi(column_length[j]), value);
							}
							else if ((strlen(value) == 0))
							{
								printf("%-*s | ", atoi(column_length[j]), " -");
							}
						}
						// account for null
						free(value);
						j++;
					}
					free(record);
					record = NULL;
					recordOffset = 0;
					j = 0;
					printf("\n");
				}
				// print end divider
				print_separator(old_header->record_size);
			}
		}
		else
		{
			// store first table name
			strcpy(tablename, cur->tok_string);
			cur = cur->next;
			if (((cur->tok_value) == K_NATURAL) && ((cur->next->tok_value) != K_JOIN))
			{
				rc = INVALID_SELECT_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;
				if (((cur->tok_value) == K_JOIN) && ((cur->next->tok_value) == EOC))
				{
					rc = INVALID_SELECT_DEFINITION;
					cur->tok_value = INVALID;
				}
				else
				{
					if (!rc)
					{
						cur = cur->next;
						strcpy(table2name, cur->tok_string);
						tab_entry = get_tpd_from_list(tablename);
						tab2_entry = get_tpd_from_list(table2name);
						// get column info for table 1
						for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
						{
							strcpy(column_names[i], col_entry->col_name);
							sprintf(column_length[i], "%d", col_entry->col_len);
							sprintf(column_type[i], "%d", col_entry->col_type);
						}

						// get column info for table 2
						for (i = 0, col_entry2 = (cd_entry *)((char *)tab2_entry + tab2_entry->cd_offset); i < tab2_entry->num_columns; i++, col_entry2++)
						{
							strcpy(column_names2[i], col_entry2->col_name);
							sprintf(column_length2[i], "%d", col_entry2->col_len);
							sprintf(column_type2[i], "%d", col_entry2->col_type);
						}

						bool done = false;
						int index1 = 0;
						int index2 = 0;
						int tab1_num_cols = tab_entry->num_columns;
						int tab2_num_cols = tab2_entry->num_columns;
						int table1_common_col = 0;
						int table2_common_col = 0;
						while (!done)
						{
							// travered all the columns
							if ((index1 == tab1_num_cols) && (index2 == tab2_num_cols))
							{
								done = true;
							}
							else if (strcmp(column_names[index1], column_names2[index2]) == 0)
							{
								// common column
								if (atoi(column_type[index1]) == T_INT)
								{
									printf("%*s", atoi(column_length[index1]), column_names[index1]);
								}
								else
								{
									printf("%-*s", atoi(column_length[index1]), column_names[index1]);
								}
								table1_common_col = index1;
								table2_common_col = index2;
								index1++;
								index2++;
							}
							// print table1 column names first
							else if (index1 < tab1_num_cols)
							{
								if (atoi(column_type[index1]) == T_INT)
								{
									printf("%*s", atoi(column_length[index1]), column_names[index1]);
								}
								else
								{
									printf("%-*s", atoi(column_length[index1]), column_names[index1]);
								}
								index1++;
							}
							else if (index2 < tab2_num_cols)
							{
								if (atoi(column_type2[index2]) == T_INT)
								{
									printf("%*s", atoi(column_length2[index2]), column_names2[index2]);
								}
								else
								{
									printf("%-*s", atoi(column_length2[index2]), column_names2[index2]);
								}
								index2++;
							}
						}
						// Read the files
						strcpy(filename, strcat(tablename, ".tab"));
						strcpy(filename2, strcat(table2name, ".tab"));
						if (((fhandle = fopen(filename, "rbc")) == NULL))
						{
							rc = FILE_OPEN_ERROR;
						}
						else
						{
							fstat(fileno(fhandle), &file_stat);
							old_header = (table_file_header *)calloc(1, file_stat.st_size);
							fread((void *)((char *)old_header), file_stat.st_size, 1, fhandle);
							if (((fhandle2 = fopen(filename2, "rbc")) == NULL))
							{
								rc = FILE_OPEN_ERROR;
							}
							else
							{
								fstat(fileno(fhandle2), &file2_stat);
								old_header2 = (table_file_header *)calloc(1, file2_stat.st_size);
								fread((void *)((char *)old_header2), file2_stat.st_size, 1, fhandle2);
								char *tab1_record = NULL;
								char *tab2_record = NULL;
								char *tab1_value;
								char *tab2_value;
								bool n_join_done = false;
								/*for (i = 0; i < old_header->num_records; i++)
								{
									if (n_join_done)
									{
										break;
									}
									tab1_record = (char *)calloc(1, old_header->record_size);
									memcpy((void *)((char *)tab1_record), (void *)((char *)old_header + old_header->record_offset), old_header->record_size);
									for (k = 0; k < old_header2->num_records; k++)
									{
										tab2_record = (char *)calloc(1, old_header2->record_size);
										memcpy((void *)((char *)tab2_record), (void *)((char *)old_header2 + old_header2->record_offset), old_header2->record_size);
										for (m = 0; m < tab_entry->num_columns; m++)
										{
											// check for matching column
											if (m != table1_common_col)
											{
												continue;
											}
											else
											{
												// print column values
												// common column
												for (int n = 0; n < tab2_entry->num_columns; n++)
												{
													if (n == table2_common_col)
													{
														continue;
													}
													else
													{
														// print rest of column values from second table
													}
												}
											}
										}

										free(tab2_record);
										tab2_record = NULL;
									}
									free(tab1_record);
									tab1_record = NULL;
								}
								printf("");*/
							}
						}
					}
				}
			}
		}
	}

	return rc;
}

int sem_delete(token_list *t_list)
{
	int rc = 0;
	printf("PREPARING TO DELETE\n");
	return rc;
}

int sem_update(token_list *t_list)
{
	int rc = 0;
	printf("PREPARING TO UPDATE\n");
	return rc;
}

void print_separator(int n)
{
	int i = 0;
	int record_size = n;
	while (i < record_size)
	{
		if (i == record_size - 1)
		{
			printf("-\n");
		}
		else
		{
			printf("-");
		}
		i++;
	}
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	//	struct _stat file_stat;
	struct stat file_stat;

	/* Open for read */
	if ((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list *)calloc(1, sizeof(tpd_list));

			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		//		_fstat(_fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list *)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}
		}
	}

	return rc;
}

int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
			g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if ((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void *)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								   1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char *)cur + cur->tpd_size,
								   old_size - cur->tpd_size -
									   (sizeof(tpd_list) - sizeof(tpd_entry)),
								   1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
						g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char *)cur - (char *)g_tpd_list),
							   1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char *)g_tpd_list + g_tpd_list->list_size == (char *)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char *)cur + cur->tpd_size,
								   old_size - cur->tpd_size -
									   ((char *)cur - (char *)g_tpd_list),
								   1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry *)((char *)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}

	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry *get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry *)((char *)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}