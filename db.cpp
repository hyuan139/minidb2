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
	token_list *cur = t_list;
	// check correct select syntax
	if ((cur->tok_value) == S_STAR)
	{
		// star symbol -> next token
		cur = cur->next;
		if ((cur->tok_value) != K_FROM)
		{
			rc = INVALID_SELECT_DEFINITION;
			cur->tok_class = error;
			cur->tok_value = INVALID;
		}
		else
		{
			rc = sem_select_star(cur);
		}
	}
	else if ((cur->tok_value == F_SUM) || (cur->tok_value == F_AVG) || (cur->tok_value == F_COUNT))
	{
		rc = sem_select_aggregate(cur);
	}
	else if (((cur->tok_value != F_SUM) || (cur->tok_value != F_AVG) || (cur->tok_value != F_COUNT)) && ((cur->next->tok_value == S_LEFT_PAREN) || (cur->next->tok_value == S_RIGHT_PAREN)))
	{
		rc = INVALID_SELECT_DEFINITION;
		cur->tok_class = error;
		cur->tok_value = INVALID;
	}
	else
	{
		// statement may be a projection
		rc = sem_select_project(cur);
	}
	return rc;
}

int sem_select_star(token_list *t_list)
{
	int rc = 0;
	FILE *fhandle = NULL;
	token_list *cur = t_list;
	tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;
	table_file_header *old_header = NULL;
	struct stat file_stat;
	char filename[MAX_TOK_LEN + 4];
	char tablename[MAX_TOK_LEN];
	char column_names[MAX_NUM_COL][MAX_IDENT_LEN];
	char column_length[MAX_NUM_COL][MAX_IDENT_LEN];
	char column_type[MAX_NUM_COL][MAX_IDENT_LEN];
	int i;
	int length_for_print = 0;
	int length_arr_indexes[MAX_NUM_COL];
	int num_col_index = 0;
	int sum_table_length = 0;
	printf("SELECT * statement\n");
	// from keyword -> next token
	cur = cur->next; // table name
	strcpy(tablename, cur->tok_string);
	strcpy(filename, strcat(cur->tok_string, ".tab"));
	cur = cur->next;
	// Regular SELECT statement with no options
	if ((cur->tok_value == EOC))
	{
		tab_entry = get_tpd_from_list(tablename);
		for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
		{
			// store column name and column length in two arrays to retrieve value later for format
			strcpy(column_names[i], col_entry->col_name);
			sprintf(column_length[i], "%d", col_entry->col_len);
			sprintf(column_type[i], "%d", col_entry->col_type);
			if (length_for_print < atoi(column_length[i]))
			{
				// set the leader length value
				length_for_print = atoi(column_length[i]);
			}
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
			int j = 0;
			int length_for_small_column = 0;
			if (length_for_print < 10)
			{
				length_for_small_column = length_for_print * 2;
			}
			else
			{
				length_for_small_column = length_for_print / 2; // length for the table divider for small column lengths
			}

			// TODO: Fix Formatting later
			for (i = 0; i < tab_entry->num_columns; i++)
			{
				if (atoi(column_type[i]) == T_INT)
				{
					length_arr_indexes[i] = length_for_small_column;
					sum_table_length += length_for_small_column + 2;
				}
				else if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
				{
					if (atoi(column_length[i]) < 5)
					{
						length_arr_indexes[i] = length_for_small_column;
						sum_table_length += length_for_small_column + 2;
					}
					else
					{
						length_arr_indexes[i] = atoi(column_length[i]);
						sum_table_length += atoi(column_length[i]) + 2;
					}
				}
			}
			print_separator(sum_table_length + 1);
			while (j < tab_entry->num_columns)
			{
				if ((j == tab_entry->num_columns - 1) && (atoi(column_type[j]) == T_INT))
				{
					printf("%*s", length_arr_indexes[j], column_names[j]);
				}
				else if ((j == tab_entry->num_columns - 1) && ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR)))
				{
					printf("%-*s|", length_arr_indexes[j], column_names[j]);
				}
				else if ((atoi(column_type[j]) == T_INT))
				{
					printf("%*s | ", length_arr_indexes[j], column_names[j]);
				}
				else if (((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR)))
				{
					printf("%-*s | ", length_arr_indexes[j], column_names[j]);
				}
				j++;
			}
			printf("\n");
			print_separator(sum_table_length + 1);
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
							printf("%*ld", length_arr_indexes[j], decimal);
						}
						else if ((j == tab_entry->num_columns - 1) && (decimal == 0))
						{
							printf("%*s", length_arr_indexes[j], " -");
						}
						else if ((decimal != 0))
						{
							printf("%*ld | ", length_arr_indexes[j], decimal);
						}
						else if ((decimal == 0))
						{
							printf("%*s | ", length_arr_indexes[j], " -");
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
							printf("%-*s", length_arr_indexes[j], value);
						}
						else if ((j == tab_entry->num_columns - 1) && (strlen(value) == 0))
						{
							printf("%-*s", length_arr_indexes[j], " -");
						}
						else if ((strlen(value) != 0))
						{
							printf("%-*s | ", length_arr_indexes[j], value);
						}
						else if ((strlen(value) == 0))
						{
							printf("%-*s | ", length_arr_indexes[j], " -");
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
			print_separator(sum_table_length + 1);
		}
	}
	// check if statement has natural join option
	else if ((cur->tok_value == K_NATURAL) && (cur->next->tok_value == K_JOIN))
	{
		cur = cur->next->next;
		printf("Has natural join keyword\n");
		printf("Token: %s\n", cur->tok_string);
		// TODO: Further processing
	}
	else if (cur->tok_value == K_WHERE)
	{
		cur = cur->next;
		printf("Statement has WHERE\n");
		printf("Token: %s\n", cur->tok_string);
		// TODO: Further processing
	}
	else if ((cur->tok_value == K_ORDER) && (cur->next->tok_value == K_BY))
	{
		cur = cur->next->next;
		if (cur->tok_value == K_DESC)
		{
			printf("Statement has ORDER BY %s\n", cur->tok_string);
		}
		else
		{
			rc = INVALID_SELECT_DEFINITION;
			cur->tok_class = error;
			cur->tok_value = INVALID;
		}
		// TODO: Further processing
	}
	else
	{
		rc = INVALID_SELECT_DEFINITION;
		cur->tok_class = error;
		cur->tok_value = INVALID;
	}
	return rc;
}

int sem_select_project(token_list *t_list)
{
	int rc = 0;
	FILE *fhandle = NULL;
	token_list *cur = t_list;
	tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;
	table_file_header *old_header = NULL;
	struct stat file_stat;
	char filename[MAX_TOK_LEN + 4];
	char tablename[MAX_TOK_LEN];
	char column_names[MAX_NUM_COL][MAX_TOK_LEN];
	char column_length[MAX_NUM_COL][MAX_TOK_LEN];
	char column_type[MAX_NUM_COL][MAX_TOK_LEN];
	int i = 0;
	int length_for_print = 0;
	int length_arr_indexes[MAX_NUM_COL];
	int num_col_index = 0;
	int sum_table_length = 0;
	bool get_projected_columns_done = false;
	char projected_columnNames[MAX_NUM_COL][MAX_TOK_LEN];
	bool get_condition_columns_done = false;
	char condition_columnNames[MAX_NUM_COL][MAX_TOK_LEN];
	int condition_operator[MAX_NUM_COL]; // 0 -> equal, 1 -> less than, 2 -> greater than, 3 -> IS NULL, 4 IS NOT NULL
	int boolean_operator[MAX_NUM_COL];	 // 0 -> AND, 1 -> OR
	char condition_values_strings[MAX_NUM_COL][MAX_TOK_LEN];
	char condition_values_ints[MAX_NUM_COL];
	if ((cur->tok_value == STRING_LITERAL) || (cur->tok_value == INT_LITERAL))
	{
		rc = INVALID_SELECT_DEFINITION;
		cur->tok_class = error;
		cur->tok_value = INVALID;
	}
	else
	{
		// make sure first token after select is not a comma
		if (cur->tok_value == S_COMMA)
		{
			rc = INVALID_SELECT_DEFINITION;
			cur->tok_class = error;
			cur->tok_value = INVALID;
		}
		else
		{
			while (!get_projected_columns_done)
			{
				if ((cur->next->tok_value == S_COMMA) || (cur->next->tok_value == K_FROM))
				{
					if (cur->next->tok_value == K_FROM)
					{
						get_projected_columns_done = true;
					}
					strcpy(projected_columnNames[i], cur->tok_string);
					cur = cur->next->next;
					i++;
				}
				else
				{
					rc = INVALID_SELECT_DEFINITION;
					cur->tok_class = error;
					cur->tok_value = INVALID;
				}
			}
			// table name
			strcpy(tablename, cur->tok_string);
			strcpy(filename, strcat(tablename, ".tab"));
			cur = cur->next;
			if (cur->tok_value == EOC)
			{
				printf("SELECT statement with projection (VALID, no other options)\n");
				// Regular select with projection on columns
			}
			else if ((cur->tok_value == K_NATURAL) && (cur->next->tok_value == K_JOIN))
			{
				cur = cur->next->next;
				printf("Has natural join keyword\n");
				printf("Token: %s\n", cur->tok_string);
				// TODO: Further processing
			}
			else if (cur->tok_value == K_WHERE)
			{
				cur = cur->next;
				printf("Statement has WHERE\n");
				printf("Token: %s\n", cur->tok_string);
				//  TODO: Further processing
				i = 0; // reset
				do
				{
					strcpy(condition_columnNames[i], cur->tok_string); // copy column name
					cur = cur->next;
					// check for relational operator
					if (cur->tok_value == S_EQUAL)
					{
						condition_operator[i] = 0;
						cur = cur->next;
						if (cur->tok_value == INT_LITERAL)
						{
							condition_values_ints[i] = atoi(cur->tok_string);
							cur = cur->next;
						}
						else if (cur->tok_value == STRING_LITERAL)
						{
							printf("Token: %s\n", cur->tok_string);
							strcpy(condition_values_strings[i], cur->tok_string);
							cur = cur->next;
						}
						else
						{
							rc = INVALID_SELECT_DEFINITION;
							cur->tok_class = error;
							cur->tok_value = INVALID;
						}
					}
					else if (cur->tok_value == S_LESS)
					{
						condition_operator[i] = 1;
						cur = cur->next;
						if (cur->tok_value == INT_LITERAL)
						{
							condition_values_ints[i] = atoi(cur->tok_string);
							cur = cur->next;
						}
						else if (cur->tok_value == STRING_LITERAL)
						{
							strcpy(condition_values_strings[i], cur->tok_string);
							cur = cur->next;
						}
						else
						{
							rc = INVALID_SELECT_DEFINITION;
							cur->tok_class = error;
							cur->tok_value = INVALID;
						}
					}
					else if (cur->tok_value == S_GREATER)
					{
						condition_operator[i] = 2;
						cur = cur->next;
						if (cur->tok_value == INT_LITERAL)
						{
							condition_values_ints[i] = atoi(cur->tok_string);
							cur = cur->next;
						}
						else if (cur->tok_value == STRING_LITERAL)
						{
							strcpy(condition_values_strings[i], cur->tok_string);
							cur = cur->next;
						}
						else
						{
							rc = INVALID_SELECT_DEFINITION;
							cur->tok_class = error;
							cur->tok_value = INVALID;
						}
					}
					else if ((cur->tok_value == K_IS) && (cur->tok_value == K_NULL))
					{
						condition_operator[i] = 3;
						cur = cur->next->next;
					}
					else if ((cur->tok_value == K_IS) && (cur->tok_value == K_NOT) && (cur->tok_value == K_NULL))
					{
						condition_operator[i] = 4;
						cur = cur->next->next->next;
					}
					if ((cur->tok_value == K_AND) || (cur->tok_value == K_OR))
					{
						if (cur->tok_value == K_AND)
						{
							printf("Token: %s\n", cur->tok_string);
							boolean_operator[i] = 0;
							cur = cur->next;
						}
						else
						{
							printf("Token: %s\n", cur->tok_string);
							boolean_operator[i] = 1;
							cur = cur->next;
						}
					}
					if (cur->tok_value == EOC)
					{
						get_condition_columns_done = true;
					}
					i++;

				} while (!get_condition_columns_done);
			}
			else if ((cur->tok_value == K_ORDER) && (cur->next->tok_value == K_BY))
			{
				cur = cur->next->next;

				if (cur->tok_value == K_DESC)
				{
					printf("Statement has ORDER BY %s\n", cur->tok_string);
				}
				else
				{
					rc = INVALID_SELECT_DEFINITION;
					cur->tok_class = error;
					cur->tok_value = INVALID;
				}

				// TODO: Further processing
			}
			else
			{
				rc = INVALID_SELECT_DEFINITION;
				cur->tok_class = error;
				cur->tok_value = INVALID;
			}
			// validate column exists
		}
	}
	return rc;
}

int sem_select_aggregate(token_list *t_list)
{
	// only one row returned?
	int rc = 0;
	FILE *fhandle = NULL;
	token_list *cur = t_list;
	tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;
	table_file_header *old_header = NULL;
	struct stat file_stat;
	char filename[MAX_TOK_LEN + 4];
	char tablename[MAX_TOK_LEN];
	char column_names[MAX_NUM_COL][MAX_IDENT_LEN];
	char column_length[MAX_NUM_COL][MAX_IDENT_LEN];
	char column_type[MAX_NUM_COL][MAX_IDENT_LEN];
	int i;
	int length_for_print = 0;
	int length_arr_indexes[MAX_NUM_COL];
	int num_col_index = 0;
	int sum_table_length = 0;
	char aggregate_column_name[MAX_TOK_LEN];
	bool valid_aggregate_column = false;
	bool column_exists = false;
	printf("SELECT statement with aggregate\n");
	if (cur->tok_value == F_SUM)
	{
		// sum aggregate
		cur = cur->next;
		if (cur->tok_value != S_LEFT_PAREN)
		{
			rc = INVALID_SELECT_DEFINITION;
			cur->tok_class = error;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			strcpy(aggregate_column_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_RIGHT_PAREN)
			{
				rc = INVALID_SELECT_DEFINITION;
				cur->tok_class = error;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;
				if (cur->tok_value != K_FROM)
				{
					rc = INVALID_SELECT_DEFINITION;
					cur->tok_class = error;
					cur->tok_value = INVALID;
				}
				else
				{
					cur = cur->next;
					strcpy(tablename, cur->tok_string);
					tab_entry = get_tpd_from_list(tablename); // get pointer to table before concat
					strcpy(filename, strcat(tablename, ".tab"));
					cur = cur->next;
					if (cur->tok_value == EOC)
					{
						printf("Valid sum aggregate command with no other options\n");
						// read the table, check if column name exists && check if column is a valid integer column, if not error out
						for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
						{
							strcpy(column_names[i], col_entry->col_name);
							sprintf(column_length[i], "%d", col_entry->col_len);
							sprintf(column_type[i], "%d", col_entry->col_type);
						}
						for (i = 0; i < tab_entry->num_columns; i++)
						{
							if (strcmp(aggregate_column_name, column_names[i]) == 0)
							{
								column_exists = true;
								if (atoi(column_type[i]) == T_INT)
								{
									valid_aggregate_column = true;
									break;
								}
							}
						}
						if (column_exists)
						{
							printf("Column exists.\n");
							if (valid_aggregate_column)
							{
								printf("Valid aggregate on column\n");
							}
							else
							{
								printf("NO Valid aggregate on column\n");
							}
						}
						else
						{
							rc = COLUMN_NOT_EXIST;
							cur->tok_class = error;
							cur->tok_value = INVALID;
						}
					}
					else
					{
						if ((cur->tok_value == K_NATURAL) && (cur->next->tok_value == K_JOIN))
						{
							cur = cur->next->next;
							printf("Valid sum aggregate with natural join and possible other options\n");
						}
						else if (cur->tok_value == K_WHERE)
						{
							cur = cur->next;
							printf("Valid sum aggregate with where condition and possible other options\n");
						}
						else
						{
							rc = INVALID_SELECT_DEFINITION;
							cur->tok_class = error;
							cur->tok_value = INVALID;
						}
					}
				}
			}
		}
	}
	else if (cur->tok_value == F_AVG)
	{
		// avg aggregtae
		cur = cur->next;
		if (cur->tok_value != S_LEFT_PAREN)
		{
			rc = INVALID_SELECT_DEFINITION;
			cur->tok_class = error;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			strcpy(aggregate_column_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_RIGHT_PAREN)
			{
				rc = INVALID_SELECT_DEFINITION;
				cur->tok_class = error;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;
				if (cur->tok_value != K_FROM)
				{
					rc = INVALID_SELECT_DEFINITION;
					cur->tok_class = error;
					cur->tok_value = INVALID;
				}
				else
				{
					cur = cur->next;
					strcpy(tablename, cur->tok_string);
					tab_entry = get_tpd_from_list(tablename); // get pointer to table before concat
					strcpy(filename, strcat(tablename, ".tab"));
					cur = cur->next;
					if (cur->tok_value == EOC)
					{
						printf("Valid avg aggregate command with no other options\n");
						// read the table, check if column name exists && check if column is a valid integer column, if not error out
						for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
						{
							strcpy(column_names[i], col_entry->col_name);
							sprintf(column_length[i], "%d", col_entry->col_len);
							sprintf(column_type[i], "%d", col_entry->col_type);
						}
						for (i = 0; i < tab_entry->num_columns; i++)
						{
							if (strcmp(aggregate_column_name, column_names[i]) == 0)
							{
								column_exists = true;
								if (atoi(column_type[i]) == T_INT)
								{
									valid_aggregate_column = true;
									break;
								}
							}
						}
						if (column_exists)
						{
							printf("Column exists.\n");
							if (valid_aggregate_column)
							{
								printf("Valid aggregate on column\n");
							}
							else
							{
								printf("NO Valid aggregate on column\n");
							}
						}
						else
						{
							rc = COLUMN_NOT_EXIST;
							cur->tok_class = error;
							cur->tok_value = INVALID;
						}
					}
					else
					{
						if ((cur->tok_value == K_NATURAL) && (cur->next->tok_value == K_JOIN))
						{
							cur = cur->next->next;
							printf("Valid avg aggregate with natural join and possible other options\n");
						}
						else if (cur->tok_value == K_WHERE)
						{
							cur = cur->next;
							printf("Valid avg aggregate with where condition and possible other options\n");
						}
						else
						{
							rc = INVALID_SELECT_DEFINITION;
							cur->tok_class = error;
							cur->tok_value = INVALID;
						}
					}
				}
			}
		}
	}
	else if ((cur->tok_value == F_COUNT) && (cur->next->tok_value == S_LEFT_PAREN) && (cur->next->next->tok_value == S_STAR))
	{
		// count aggregate on star operator
		cur = cur->next->next->next;
		if (cur->tok_value != S_RIGHT_PAREN)
		{
			rc = INVALID_SELECT_DEFINITION;
			cur->tok_class = error;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if (cur->tok_value != K_FROM)
			{
				rc = INVALID_SELECT_DEFINITION;
				cur->tok_class = error;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;
				strcpy(tablename, cur->tok_string);
				tab_entry = get_tpd_from_list(tablename); // get pointer to table before concat
				strcpy(filename, strcat(tablename, ".tab"));
				cur = cur->next;
				if (cur->tok_value == EOC)
				{
					printf("Valid count(*) aggregate with no options\n");
				}
				else
				{
					if ((cur->tok_value == K_NATURAL) && (cur->next->tok_value == K_JOIN))
					{
						cur = cur->next->next;
						printf("Valid count aggregate with natural join and possible other options\n");
					}
					else if (cur->tok_value == K_WHERE)
					{
						cur = cur->next;
						printf("Valid count aggregate with where condition and possible other options\n");
					}
					else
					{
						rc = INVALID_SELECT_DEFINITION;
						cur->tok_class = error;
						cur->tok_value = INVALID;
					}
				}
			}
		}
	}
	else
	{
		// count aggregate on a specific column
		cur = cur->next;
		if (cur->tok_value != S_LEFT_PAREN)
		{
			rc = INVALID_SELECT_DEFINITION;
			cur->tok_class = error;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			strcpy(aggregate_column_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_RIGHT_PAREN)
			{
				rc = INVALID_SELECT_DEFINITION;
				cur->tok_class = error;
				cur->tok_value = INVALID;
			}
			else
			{
				cur = cur->next;
				if (cur->tok_value != K_FROM)
				{
					rc = INVALID_SELECT_DEFINITION;
					cur->tok_class = error;
					cur->tok_value = INVALID;
				}
				else
				{
					cur = cur->next;
					strcpy(tablename, cur->tok_string);
					tab_entry = get_tpd_from_list(tablename); // get pointer to table before concat
					strcpy(filename, strcat(tablename, ".tab"));
					cur = cur->next;
					if (cur->tok_value == EOC)
					{
						printf("Valid count aggregate command with no other options\n");
						// read the table, check if column name exists && check if column is a valid integer column, if not error out
						for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
						{
							strcpy(column_names[i], col_entry->col_name);
							sprintf(column_length[i], "%d", col_entry->col_len);
							sprintf(column_type[i], "%d", col_entry->col_type);
						}
						for (i = 0; i < tab_entry->num_columns; i++)
						{
							if (strcmp(aggregate_column_name, column_names[i]) == 0)
							{
								column_exists = true;
								break;
							}
						}
						if (column_exists)
						{
							printf("Column exists.\n");
						}
						else
						{
							rc = COLUMN_NOT_EXIST;
							cur->tok_class = error;
							cur->tok_value = INVALID;
						}
					}
					else
					{
						if ((cur->tok_value == K_NATURAL) && (cur->next->tok_value == K_JOIN))
						{
							cur = cur->next->next;
							printf("Valid count aggregate with natural join and possible other options\n");
						}
						else if (cur->tok_value == K_WHERE)
						{
							cur = cur->next;
							printf("Valid count aggregate with where condition and possible other options\n");
						}
						else
						{
							rc = INVALID_SELECT_DEFINITION;
							cur->tok_class = error;
							cur->tok_value = INVALID;
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
	token_list *cur;
	cur = t_list;
	table_file_header *old_header = NULL;
	table_file_header *new_header = NULL;
	FILE *fhandle = NULL;
	tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;
	char column_names[MAX_NUM_COL][MAX_IDENT_LEN];
	char column_length[MAX_NUM_COL][MAX_IDENT_LEN];
	char column_type[MAX_NUM_COL][MAX_IDENT_LEN];
	struct stat file_stat;
	char filename[MAX_IDENT_LEN + 4];
	char tablename[MAX_IDENT_LEN + 1];
	int i;
	int cond_val = NULL;
	char cond_string[MAX_IDENT_LEN]; // may need to adjust size
	int records_to_delete = 0;
	char *records = NULL;
	int records_to_save_indexes[100];
	int index_last_row_to_save;
	int index = 0;
	bool table_empty = false;
	bool column_exists = false;
	bool column_is_type_int = false;
	bool column_is_type_string = false;
	strcpy(tablename, cur->tok_string);
	strcpy(filename, strcat(cur->tok_string, ".tab"));
	int num_records_affected = 0;
	if (cur->next->tok_value == K_WHERE)
	{
		cur = cur->next->next;
		printf("Delete with WHERE condition\n");
		if ((cur->tok_value == S_EQUAL) || (cur->tok_value == INT_LITERAL) || (cur->tok_value == STRING_LITERAL))
		{
			rc = INVALID_DELETE_DEFINITION;
			cur->tok_class = error;
			cur->tok_value = INVALID;
		}
		else
		{
			char colName[MAX_IDENT_LEN];
			strcpy(colName, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value == S_EQUAL)
			{
				cur = cur->next;
				if (cur->tok_value == INT_LITERAL)
				{
					cond_val = atoi(cur->tok_string);
					if (cur->next->tok_value != EOC)
					{
						rc = INVALID_DELETE_DEFINITION;
						cur->tok_class = error;
						cur->tok_value = INVALID;
					}
					else
					{
						// command looks good, proceed
						if ((fhandle = fopen(filename, "rbc")) == NULL)
						{
							printf("Error while opening %s file\n", filename);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
						}
						else
						{
							// Read the old header information from the tab file.
							fstat(fileno(fhandle), &file_stat);
							old_header = (table_file_header *)calloc(1, file_stat.st_size);
							fread(old_header, file_stat.st_size, 1, fhandle);
							// get table information
							tab_entry = get_tpd_from_list(tablename);
							for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
							{
								strcpy(column_names[i], col_entry->col_name);
								sprintf(column_length[i], "%d", col_entry->col_len);
								sprintf(column_type[i], "%d", col_entry->col_type);
							}
							for (i = 0; i < tab_entry->num_columns; i++)
							{
								if (strcmp(colName, column_names[i]) == 0)
								{
									column_exists = true;
									if (atoi(column_type[i]) == T_INT)
									{
										column_is_type_int = true;
									}
								}
							}
							if (column_exists && column_is_type_int)
							{
								// copy records to buffer
								records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
								memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
								char *record = NULL;
								char *value;
								int j = 0;
								int columnOffset = 0;
								for (i = 0; i < old_header->num_records; i++)
								{
									record = (char *)calloc(1, old_header->record_size);
									memcpy((void *)((char *)record), (void *)((char *)old_header + old_header->record_offset + (i * old_header->record_size)), old_header->record_size);
									while (j < tab_entry->num_columns)
									{
										value = NULL;
										// found matching column name
										columnOffset += 1; // account for length byte
										if (strcmp(colName, column_names[j]) == 0)
										{
											if (atoi(column_type[j]) == T_INT)
											{
												value = (char *)calloc(1, sizeof(int));
												memcpy((void *)((char *)value), (void *)((char *)record + columnOffset), sizeof(int));
												char hexValue[16];
												char temp[16];
												memset(hexValue, '\0', 16);
												strcat(hexValue, "0x");
												sprintf(temp, "%x", (int)value[1]);
												strcat(hexValue, temp);
												sprintf(temp, "%x", (int)value[0]);
												strcat(hexValue, temp);
												long decimal = strtol(hexValue, NULL, 16);
												if (cond_val == decimal)
												{
													records_to_delete += 1;
												}
												else
												{
													records_to_save_indexes[index] = i;
													index++;
													if (i == old_header->num_records - 1)
													{
														index_last_row_to_save = i;
													}
												}
											}
										}
										else
										{
											columnOffset += atoi(column_length[j]);
										}
										j++;
									}
									// reset
									free(record);
									record = NULL;
									columnOffset = 0;
									j = 0;
								}
								fclose(fhandle);
								if (records_to_delete == 0)
								{
									printf("WARNING! No row is found.\n");
								}
								else
								{
									// Process to delete here
									num_records_affected = records_to_delete;
									int k = 0;
									int newHeaderSize = old_header->file_size - (records_to_delete * old_header->record_size);
									new_header = (table_file_header *)calloc(1, newHeaderSize);
									memcpy((void *)((char *)new_header), (void *)old_header, old_header->record_offset);
									new_header->num_records = old_header->num_records - records_to_delete;
									new_header->file_size = newHeaderSize;
									char *record = NULL;
									char *value;
									int columnOffset = 0;
									int index2 = index - 1;
									for (int j = 0; j < old_header->num_records; j++)
									{
										while ((k < tab_entry->num_columns) && !table_empty)
										{
											value = NULL;
											columnOffset += 1; // account for length byte
											if (strcmp(colName, column_names[k]) == 0)
											{
												if (atoi(column_type[k]) == T_INT)
												{
													value = (char *)calloc(1, sizeof(int));
													// memcpy((void *)((char *)value), (void *)((char *)records + (j * old_header->record_size) + columnOffset + (k * sizeof(int))), sizeof(int));
													//  columnOffset += sizeof(int);
													memcpy((void *)((char *)value), (void *)((char *)records + (j * old_header->record_size) + columnOffset), sizeof(int));
													char hexValue[16];
													char temp[16];
													memset(hexValue, '\0', 16);
													strcat(hexValue, "0x");
													sprintf(temp, "%x", (int)value[1]);
													strcat(hexValue, temp);
													sprintf(temp, "%x", (int)value[0]);
													strcat(hexValue, temp);
													long decimal = strtol(hexValue, NULL, 16);
													if (cond_val == decimal)
													{
														// printf("\nFound value: %ld", decimal);
														if (records_to_delete != 0)
														{
															if (new_header->num_records == 0)
															{
																table_empty = true;
															}
															else
															{
																// proceed delete as normal
																memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (records_to_save_indexes[index2] * old_header->record_size)), old_header->record_size);
																index2--;
																records_to_delete--;
															}
														}
													}
													else if ((j < new_header->num_records) && !table_empty)
													{
														memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (j * old_header->record_size)), old_header->record_size);
													}
												}
											}
											else
											{
												columnOffset += atoi(column_length[k]);
											}
											k++;
										}
										// reset
										free(record);
										record = NULL;
										columnOffset = 0;
										k = 0;
									}
									if ((fhandle = fopen(filename, "wbc")) == NULL)
									{
										rc = FILE_OPEN_ERROR;
									}
									else
									{
										fwrite(new_header, newHeaderSize, 1, fhandle);
										printf("Delete Sucess! Number of rows affected (%d)\n", num_records_affected);
									}
									free(records);
									free(value);
								}
							}
							else
							{
								rc = COLUMN_NOT_EXIST;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
						}
					}
				}
				else if (cur->tok_value == STRING_LITERAL)
				{
					strcpy(cond_string, cur->tok_string);
					if (cur->next->tok_value != EOC)
					{
						rc = INVALID_DELETE_DEFINITION;
						cur->tok_class = error;
						cur->tok_value = INVALID;
					}
					else
					{
						// command looks good, proceed
						if ((fhandle = fopen(filename, "rbc")) == NULL)
						{
							printf("Error while opening %s file\n", filename);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
						}
						else
						{
							// Read the old header information from the tab file.
							fstat(fileno(fhandle), &file_stat);
							old_header = (table_file_header *)calloc(1, file_stat.st_size);
							fread(old_header, file_stat.st_size, 1, fhandle);
							// get table information
							tab_entry = get_tpd_from_list(tablename);
							for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
							{
								strcpy(column_names[i], col_entry->col_name);
								sprintf(column_length[i], "%d", col_entry->col_len);
								sprintf(column_type[i], "%d", col_entry->col_type);
							}
							for (i = 0; i < tab_entry->num_columns; i++)
							{
								if (strcmp(colName, column_names[i]) == 0)
								{
									column_exists = true;
									if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
									{
										column_is_type_string = true;
									}
								}
							}
							if (column_exists && column_is_type_string)
							{
								// copy records to buffer
								records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
								memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
								char *record = NULL;
								char *value;
								int j = 0;
								int columnOffset = 0;
								for (i = 0; i < old_header->num_records; i++)
								{
									while (j < tab_entry->num_columns)
									{
										value = NULL;
										// found matching column name
										columnOffset += 1; // account for length byte
										if (strcmp(colName, column_names[j]) == 0)
										{
											if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
											{
												value = (char *)calloc(1, atoi(column_length[j]));
												memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
												if (strcmp(cond_string, value) == 0)
												{
													records_to_delete += 1;
												}
												else
												{
													records_to_save_indexes[index] = i;
													index++;
													if (i == old_header->num_records - 1)
													{
														index_last_row_to_save = i;
													}
												}
											}
										}
										else
										{
											columnOffset += atoi(column_length[j]);
										}
										j++;
									}
									// reset
									columnOffset = 0;
									j = 0;
								}
								fclose(fhandle);
								if (records_to_delete == 0)
								{
									printf("WARNING! No row is found.\n");
								}
								else
								{
									// Process to delete here
									num_records_affected = records_to_delete;
									int k = 0;
									int newHeaderSize = old_header->file_size - (records_to_delete * old_header->record_size);
									new_header = (table_file_header *)calloc(1, newHeaderSize);
									memcpy((void *)((char *)new_header), (void *)old_header, old_header->record_offset);
									new_header->num_records = old_header->num_records - records_to_delete;
									new_header->file_size = newHeaderSize;
									char *record = NULL;
									char *value;
									int columnOffset = 0;
									int index2 = index - 1;
									for (int j = 0; j < old_header->num_records; j++)
									{
										while ((k < tab_entry->num_columns) && !table_empty)
										{
											value = NULL;
											columnOffset += 1; // account for length byte
											if (strcmp(colName, column_names[k]) == 0)
											{
												if ((atoi(column_type[k]) == T_CHAR) || (atoi(column_type[k]) == T_VARCHAR))
												{
													value = (char *)calloc(1, atoi(column_length[k]));
													memcpy((void *)((char *)value), (void *)((char *)records + (j * old_header->record_size) + columnOffset), atoi(column_length[k]));

													if (strcmp(cond_string, value) == 0)
													{
														if (records_to_delete != 0)
														{
															if (new_header->num_records == 0)
															{
																table_empty = true;
															}
															else
															{
																// proceed delete as normal
																if (j != old_header->num_records - 1)
																{
																	memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (records_to_save_indexes[index2] * old_header->record_size)), old_header->record_size);
																	index2--;
																	records_to_delete--;
																}
																else
																{
																	// record to delete is already at last row
																	continue;
																}
															}
														}
													}
													else if ((j < new_header->num_records) && !table_empty)
													{
														memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (j * old_header->record_size)), old_header->record_size);
													}
												}
											}
											else
											{
												columnOffset += atoi(column_length[k]);
											}
											k++;
										}
										// reset
										free(record);
										record = NULL;
										columnOffset = 0;
										k = 0;
									}
									if ((fhandle = fopen(filename, "wbc")) == NULL)
									{
										rc = FILE_OPEN_ERROR;
									}
									else
									{
										fwrite(new_header, newHeaderSize, 1, fhandle);
										printf("Delete Sucess! Number of rows affected (%d)\n", num_records_affected);
									}
									free(records);
									free(value);
								}
							}
							else
							{
								rc = COLUMN_NOT_EXIST;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
						}
					}
				}
				else
				{
					rc = INVALID_DELETE_DEFINITION;
					cur->tok_class = error;
					cur->tok_value = INVALID;
				}
			}
			else if (cur->tok_value == S_LESS)
			{
				cur = cur->next;
				if (cur->tok_value == INT_LITERAL)
				{
					cond_val = atoi(cur->tok_string);
					if (cur->next->tok_value != EOC)
					{
						rc = INVALID_DELETE_DEFINITION;
						cur->tok_class = error;
						cur->tok_value = INVALID;
					}
					else
					{
						// command looks good, proceed
						if ((fhandle = fopen(filename, "rbc")) == NULL)
						{
							printf("Error while opening %s file\n", filename);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
						}
						else
						{
							// Read the old header information from the tab file.
							fstat(fileno(fhandle), &file_stat);
							old_header = (table_file_header *)calloc(1, file_stat.st_size);
							fread(old_header, file_stat.st_size, 1, fhandle);
							// get table information
							tab_entry = get_tpd_from_list(tablename);
							for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
							{
								strcpy(column_names[i], col_entry->col_name);
								sprintf(column_length[i], "%d", col_entry->col_len);
								sprintf(column_type[i], "%d", col_entry->col_type);
							}
							for (i = 0; i < tab_entry->num_columns; i++)
							{
								if (strcmp(colName, column_names[i]) == 0)
								{
									column_exists = true;
									if (atoi(column_type[i]) == T_INT)
									{
										column_is_type_int = true;
									}
								}
							}
							fclose(fhandle);
							if (column_exists && column_is_type_int)
							{
								// copy records to buffer
								records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
								memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
								char *record = NULL;
								char *value;
								int j = 0;
								int columnOffset = 0;
								for (i = 0; i < old_header->num_records; i++)
								{
									record = (char *)calloc(1, old_header->record_size);
									memcpy((void *)((char *)record), (void *)((char *)old_header + old_header->record_offset + (i * old_header->record_size)), old_header->record_size);
									while (j < tab_entry->num_columns)
									{
										value = NULL;
										// found matching column name
										columnOffset += 1; // account for length byte
										if (strcmp(colName, column_names[j]) == 0)
										{
											if (atoi(column_type[j]) == T_INT)
											{
												value = (char *)calloc(1, sizeof(int));
												memcpy((void *)((char *)value), (void *)((char *)record + columnOffset), sizeof(int));
												char hexValue[16];
												char temp[16];
												memset(hexValue, '\0', 16);
												strcat(hexValue, "0x");
												sprintf(temp, "%x", (int)value[1]);
												strcat(hexValue, temp);
												sprintf(temp, "%x", (int)value[0]);
												strcat(hexValue, temp);
												long decimal = strtol(hexValue, NULL, 16);
												if (decimal < cond_val)
												{
													records_to_delete += 1;
												}
												else
												{
													records_to_save_indexes[index] = i;
													index++;
													if (i == old_header->num_records - 1)
													{
														index_last_row_to_save = i;
													}
												}
											}
										}
										else
										{
											columnOffset += atoi(column_length[j]);
										}
										j++;
									}
									// reset
									free(record);
									record = NULL;
									columnOffset = 0;
									j = 0;
								}
								if (records_to_delete == 0)
								{
									printf("WARNING! No row is found.\n");
								}
								else
								{
									// Process to delete here
									num_records_affected = records_to_delete;
									int k = 0;
									int newHeaderSize = old_header->file_size - (records_to_delete * old_header->record_size);
									new_header = (table_file_header *)calloc(1, newHeaderSize);
									memcpy((void *)((char *)new_header), (void *)old_header, old_header->record_offset);
									new_header->num_records = old_header->num_records - records_to_delete;
									new_header->file_size = newHeaderSize;
									char *record = NULL;
									char *value;
									int columnOffset = 0;
									int index2 = index - 1;
									for (int j = 0; j < old_header->num_records; j++)
									{
										while ((k < tab_entry->num_columns) && !table_empty)
										{
											value = NULL;
											columnOffset += 1; // account for length byte
											if (strcmp(colName, column_names[k]) == 0)
											{
												if (atoi(column_type[k]) == T_INT)
												{
													value = (char *)calloc(1, sizeof(int));
													memcpy((void *)((char *)value), (void *)((char *)records + (j * old_header->record_size) + columnOffset), sizeof(int));
													char hexValue[16];
													char temp[16];
													memset(hexValue, '\0', 16);
													strcat(hexValue, "0x");
													sprintf(temp, "%x", (int)value[1]);
													strcat(hexValue, temp);
													sprintf(temp, "%x", (int)value[0]);
													strcat(hexValue, temp);
													long decimal = strtol(hexValue, NULL, 16);
													if (decimal < cond_val)
													{
														if (records_to_delete != 0)
														{
															if (new_header->num_records == 0)
															{
																table_empty = true;
															}
															else
															{
																if (index2 >= 0)
																{
																	// proceed delete as normal
																	memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (records_to_save_indexes[index2] * old_header->record_size)), old_header->record_size);
																	index2--;
																	records_to_delete--;
																}
															}
														}
													}
													else if ((j < new_header->num_records) && !table_empty)
													{
														memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (j * old_header->record_size)), old_header->record_size);
													}
												}
											}
											else
											{
												columnOffset += atoi(column_length[k]);
											}
											k++;
										}
										// reset
										free(record);
										record = NULL;
										columnOffset = 0;
										k = 0;
									}
									if ((fhandle = fopen(filename, "wbc")) == NULL)
									{
										rc = FILE_OPEN_ERROR;
									}
									else
									{
										fwrite(new_header, newHeaderSize, 1, fhandle);
										printf("Delete Sucess! Number of rows affected (%d)\n", num_records_affected);
									}
									free(records);
									free(value);
								}
							}
							else
							{
								rc = COLUMN_NOT_EXIST;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
						}
					}
				}
				else if (cur->tok_value == STRING_LITERAL)
				{
					strcpy(cond_string, cur->tok_string);
					if (cur->next->tok_value != EOC)
					{
						rc = INVALID_DELETE_DEFINITION;
						cur->tok_class = error;
						cur->tok_value = INVALID;
					}
					else
					{
						// command looks good, proceed
						if ((fhandle = fopen(filename, "rbc")) == NULL)
						{
							printf("Error while opening %s file\n", filename);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
						}
						else
						{
							// Read the old header information from the tab file.
							fstat(fileno(fhandle), &file_stat);
							old_header = (table_file_header *)calloc(1, file_stat.st_size);
							fread(old_header, file_stat.st_size, 1, fhandle);
							// get table information
							tab_entry = get_tpd_from_list(tablename);
							for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
							{
								strcpy(column_names[i], col_entry->col_name);
								sprintf(column_length[i], "%d", col_entry->col_len);
								sprintf(column_type[i], "%d", col_entry->col_type);
							}
							for (i = 0; i < tab_entry->num_columns; i++)
							{
								if (strcmp(colName, column_names[i]) == 0)
								{
									column_exists = true;
									if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
									{
										column_is_type_string = true;
									}
								}
							}
							fclose(fhandle);
							if (column_exists && column_is_type_string)
							{
								// copy records to buffer
								records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
								memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
								char *record = NULL;
								char *value;
								int j = 0;
								int columnOffset = 0;
								for (i = 0; i < old_header->num_records; i++)
								{
									while (j < tab_entry->num_columns)
									{
										value = NULL;
										// found matching column name
										columnOffset += 1; // account for length byte
										if (strcmp(colName, column_names[j]) == 0)
										{
											if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
											{
												value = (char *)calloc(1, atoi(column_length[j]));
												memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
												if (strcmp(value, cond_string) < 0)
												{
													records_to_delete += 1;
												}
												else
												{
													records_to_save_indexes[index] = i;
													index++;
													if (i == old_header->num_records - 1)
													{
														index_last_row_to_save = i;
													}
												}
											}
										}
										else
										{
											columnOffset += atoi(column_length[j]);
										}
										j++;
									}
									// reset
									columnOffset = 0;
									j = 0;
								}
								if (records_to_delete == 0)
								{
									printf("WARNING! No row is found.\n");
								}
								else
								{
									// Process to delete here
									num_records_affected = records_to_delete;
									int k = 0;
									int newHeaderSize = old_header->file_size - (records_to_delete * old_header->record_size);
									new_header = (table_file_header *)calloc(1, newHeaderSize);
									memcpy((void *)((char *)new_header), (void *)old_header, old_header->record_offset);
									new_header->num_records = old_header->num_records - records_to_delete;
									new_header->file_size = newHeaderSize;
									char *record = NULL;
									char *value;
									int columnOffset = 0;
									int index2 = index - 1;
									for (int j = 0; j < old_header->num_records; j++)
									{
										while ((k < tab_entry->num_columns) && !table_empty)
										{
											value = NULL;
											columnOffset += 1; // account for length byte
											if (strcmp(colName, column_names[k]) == 0)
											{
												if ((atoi(column_type[k]) == T_CHAR) || (atoi(column_type[k]) == T_VARCHAR))
												{
													value = (char *)calloc(1, atoi(column_length[k]));
													memcpy((void *)((char *)value), (void *)((char *)records + (j * old_header->record_size) + columnOffset), atoi(column_length[k]));

													if (strcmp(value, cond_string) < 0)
													{
														if (records_to_delete != 0)
														{
															if (new_header->num_records == 0)
															{
																table_empty = true;
															}
															else
															{
																// proceed delete as normal
																if (j != old_header->num_records - 1)
																{
																	if (index2 >= 0)
																	{
																		memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (records_to_save_indexes[index2] * old_header->record_size)), old_header->record_size);
																		index2--;
																		records_to_delete--;
																	}
																}
																else
																{
																	// record to delete is already at last row
																	continue;
																}
															}
														}
													}
													else if ((j < new_header->num_records) && !table_empty)
													{
														memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (j * old_header->record_size)), old_header->record_size);
													}
												}
											}
											else
											{
												columnOffset += atoi(column_length[k]);
											}
											k++;
										}
										// reset
										free(record);
										record = NULL;
										columnOffset = 0;
										k = 0;
									}
									if ((fhandle = fopen(filename, "wbc")) == NULL)
									{
										rc = FILE_OPEN_ERROR;
									}
									else
									{
										fwrite(new_header, newHeaderSize, 1, fhandle);
										printf("Delete Sucess! Number of records affected (%d)\n", num_records_affected);
									}
									free(records);
									free(value);
								}
							}
							else
							{
								rc = COLUMN_NOT_EXIST;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
						}
					}
				}
				else
				{
					rc = INVALID_DELETE_DEFINITION;
					cur->tok_class = error;
					cur->tok_value = INVALID;
				}
			}
			else if (cur->tok_value == S_GREATER)
			{
				cur = cur->next;
				if (cur->tok_value == INT_LITERAL)
				{
					cond_val = atoi(cur->tok_string);
					if (cur->next->tok_value != EOC)
					{
						rc = INVALID_DELETE_DEFINITION;
						cur->tok_class = error;
						cur->tok_value = INVALID;
					}
					else
					{
						// command looks good, proceed
						if ((fhandle = fopen(filename, "rbc")) == NULL)
						{
							printf("Error while opening %s file\n", filename);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
						}
						else
						{
							// Read the old header information from the tab file.
							fstat(fileno(fhandle), &file_stat);
							old_header = (table_file_header *)calloc(1, file_stat.st_size);
							fread(old_header, file_stat.st_size, 1, fhandle);
							// get table information
							tab_entry = get_tpd_from_list(tablename);
							for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
							{
								strcpy(column_names[i], col_entry->col_name);
								sprintf(column_length[i], "%d", col_entry->col_len);
								sprintf(column_type[i], "%d", col_entry->col_type);
							}
							for (i = 0; i < tab_entry->num_columns; i++)
							{
								if (strcmp(colName, column_names[i]) == 0)
								{
									column_exists = true;
									if (atoi(column_type[i]) == T_INT)
									{
										column_is_type_int = true;
									}
								}
							}
							fclose(fhandle);
							if (column_exists && column_is_type_int)
							{
								// copy records to buffer
								records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
								memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
								char *record = NULL;
								char *value;
								int j = 0;
								int columnOffset = 0;
								for (i = 0; i < old_header->num_records; i++)
								{
									record = (char *)calloc(1, old_header->record_size);
									memcpy((void *)((char *)record), (void *)((char *)old_header + old_header->record_offset + (i * old_header->record_size)), old_header->record_size);
									while (j < tab_entry->num_columns)
									{
										value = NULL;
										// found matching column name
										columnOffset += 1; // account for length byte
										if (strcmp(colName, column_names[j]) == 0)
										{
											if (atoi(column_type[j]) == T_INT)
											{
												value = (char *)calloc(1, sizeof(int));
												memcpy((void *)((char *)value), (void *)((char *)record + columnOffset), sizeof(int));
												char hexValue[16];
												char temp[16];
												memset(hexValue, '\0', 16);
												strcat(hexValue, "0x");
												sprintf(temp, "%x", (int)value[1]);
												strcat(hexValue, temp);
												sprintf(temp, "%x", (int)value[0]);
												strcat(hexValue, temp);
												long decimal = strtol(hexValue, NULL, 16);
												if (decimal > cond_val)
												{
													records_to_delete += 1;
												}
												else
												{
													records_to_save_indexes[index] = i;
													index++;
													if (i == old_header->num_records - 1)
													{
														index_last_row_to_save = i;
													}
												}
											}
										}
										else
										{
											columnOffset += atoi(column_length[j]);
										}
										j++;
									}
									// reset
									free(record);
									record = NULL;
									columnOffset = 0;
									j = 0;
								}
								if (records_to_delete == 0)
								{
									printf("WARNING! No row is found.\n");
								}
								else
								{
									// Process to delete here
									num_records_affected = records_to_delete;
									int k = 0;
									int newHeaderSize = old_header->file_size - (records_to_delete * old_header->record_size);
									new_header = (table_file_header *)calloc(1, newHeaderSize);
									memcpy((void *)((char *)new_header), (void *)old_header, old_header->record_offset);
									new_header->num_records = old_header->num_records - records_to_delete;
									new_header->file_size = newHeaderSize;
									char *record = NULL;
									char *value;
									int columnOffset = 0;
									int index2 = index - 1;
									for (int j = 0; j < old_header->num_records; j++)
									{
										while ((k < tab_entry->num_columns) && !table_empty)
										{
											value = NULL;
											columnOffset += 1; // account for length byte
											if (strcmp(colName, column_names[k]) == 0)
											{
												if (atoi(column_type[k]) == T_INT)
												{
													value = (char *)calloc(1, sizeof(int));
													memcpy((void *)((char *)value), (void *)((char *)records + (j * old_header->record_size) + columnOffset), sizeof(int));
													char hexValue[16];
													char temp[16];
													memset(hexValue, '\0', 16);
													strcat(hexValue, "0x");
													sprintf(temp, "%x", (int)value[1]);
													strcat(hexValue, temp);
													sprintf(temp, "%x", (int)value[0]);
													strcat(hexValue, temp);
													long decimal = strtol(hexValue, NULL, 16);
													if (decimal > cond_val)
													{
														if (records_to_delete != 0)
														{
															if (new_header->num_records == 0)
															{
																table_empty = true;
															}
															else
															{
																if (index2 >= 0)
																{
																	// proceed delete as normal
																	memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (records_to_save_indexes[index2] * old_header->record_size)), old_header->record_size);
																	index2--;
																	records_to_delete--;
																}
															}
														}
													}
													else if ((j < new_header->num_records) && !table_empty)
													{
														memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (j * old_header->record_size)), old_header->record_size);
													}
												}
											}
											else
											{
												columnOffset += atoi(column_length[k]);
											}
											k++;
										}
										// reset
										free(record);
										record = NULL;
										columnOffset = 0;
										k = 0;
									}
									if ((fhandle = fopen(filename, "wbc")) == NULL)
									{
										rc = FILE_OPEN_ERROR;
									}
									else
									{
										fwrite(new_header, newHeaderSize, 1, fhandle);
										printf("Delete Sucess! Number of records affected (%d)\n", num_records_affected);
									}
									free(records);
									free(value);
								}
							}
							else
							{
								rc = COLUMN_NOT_EXIST;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
						}
					}
				}
				else if (cur->tok_value == STRING_LITERAL)
				{
					strcpy(cond_string, cur->tok_string);
					if (cur->next->tok_value != EOC)
					{
						rc = INVALID_DELETE_DEFINITION;
						cur->tok_class = error;
						cur->tok_value = INVALID;
					}
					else
					{
						// command looks good, proceed
						if ((fhandle = fopen(filename, "rbc")) == NULL)
						{
							printf("Error while opening %s file\n", filename);
							rc = FILE_OPEN_ERROR;
							cur->tok_value = INVALID;
						}
						else
						{
							// Read the old header information from the tab file.
							fstat(fileno(fhandle), &file_stat);
							old_header = (table_file_header *)calloc(1, file_stat.st_size);
							fread(old_header, file_stat.st_size, 1, fhandle);
							// get table information
							tab_entry = get_tpd_from_list(tablename);
							for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
							{
								strcpy(column_names[i], col_entry->col_name);
								sprintf(column_length[i], "%d", col_entry->col_len);
								sprintf(column_type[i], "%d", col_entry->col_type);
							}
							for (i = 0; i < tab_entry->num_columns; i++)
							{
								if (strcmp(colName, column_names[i]) == 0)
								{
									column_exists = true;
									if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
									{
										column_is_type_string = true;
									}
								}
							}
							fclose(fhandle);
							if (column_exists && column_is_type_string)
							{
								// copy records to buffer
								records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
								memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
								char *record = NULL;
								char *value;
								int j = 0;
								int columnOffset = 0;
								for (i = 0; i < old_header->num_records; i++)
								{
									// memcpy((void *)((char *)record), (void *)((char *)old_header + old_header->record_offset + (i * old_header->record_size)), old_header->record_size);
									while (j < tab_entry->num_columns)
									{
										value = NULL;
										// found matching column name
										columnOffset += 1; // account for length byte
										if (strcmp(colName, column_names[j]) == 0)
										{
											if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
											{
												value = (char *)calloc(1, atoi(column_length[j]));
												// memcpy((void *)((char *)value), (void *)((char *)record + columnOffset + (j * sizeof(int))), sizeof(int));
												memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
												if (strcmp(value, cond_string) > 0)
												{
													records_to_delete += 1;
												}
												else
												{
													records_to_save_indexes[index] = i;
													index++;
													if (i == old_header->num_records - 1)
													{
														index_last_row_to_save = i;
													}
												}
											}
										}
										else
										{
											columnOffset += atoi(column_length[j]);
										}
										j++;
									}
									// reset
									columnOffset = 0;
									j = 0;
								}
								if (records_to_delete == 0)
								{
									printf("WARNING! No row is found.\n");
								}
								else
								{
									// Process to delete here
									num_records_affected = records_to_delete;
									int k = 0;
									int newHeaderSize = old_header->file_size - (records_to_delete * old_header->record_size);
									new_header = (table_file_header *)calloc(1, newHeaderSize);
									memcpy((void *)((char *)new_header), (void *)old_header, old_header->record_offset);
									new_header->num_records = old_header->num_records - records_to_delete;
									new_header->file_size = newHeaderSize;
									char *record = NULL;
									char *value;
									int columnOffset = 0;
									int index2 = index - 1;
									for (int j = 0; j < old_header->num_records; j++)
									{
										while ((k < tab_entry->num_columns) && !table_empty)
										{
											value = NULL;
											columnOffset += 1; // account for length byte
											if (strcmp(colName, column_names[k]) == 0)
											{
												if ((atoi(column_type[k]) == T_CHAR) || (atoi(column_type[k]) == T_VARCHAR))
												{
													value = (char *)calloc(1, atoi(column_length[k]));
													memcpy((void *)((char *)value), (void *)((char *)records + (j * old_header->record_size) + columnOffset), atoi(column_length[k]));

													if (strcmp(value, cond_string) > 0)
													{
														if (records_to_delete != 0)
														{
															if (new_header->num_records == 0)
															{
																table_empty = true;
															}
															else
															{
																// proceed delete as normal
																if (j != old_header->num_records - 1)
																{
																	if (index2 >= 0)
																	{
																		memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (records_to_save_indexes[index2] * old_header->record_size)), old_header->record_size);
																		index2--;
																		records_to_delete--;
																	}
																}
																else
																{
																	// record to delete is already at last row
																	continue;
																}
															}
														}
													}
													else if ((j < new_header->num_records) && !table_empty)
													{
														memcpy((void *)((char *)new_header + old_header->record_offset + (j * old_header->record_size)), (void *)((char *)records + (j * old_header->record_size)), old_header->record_size);
													}
												}
											}
											else
											{
												columnOffset += atoi(column_length[k]);
											}
											k++;
										}
										// reset
										free(record);
										record = NULL;
										columnOffset = 0;
										k = 0;
									}
									if ((fhandle = fopen(filename, "wbc")) == NULL)
									{
										rc = FILE_OPEN_ERROR;
									}
									else
									{
										fwrite(new_header, newHeaderSize, 1, fhandle);
										printf("Delete Sucess! Number of records affected (%d)\n", num_records_affected);
									}
									free(records);
									free(value);
								}
							}
							else
							{
								rc = COLUMN_NOT_EXIST;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
						}
					}
				}
				else
				{
					rc = INVALID_DELETE_DEFINITION;
					cur->tok_class = error;
					cur->tok_value = INVALID;
				}
			}
			else
			{
				rc = INVALID_DELETE_DEFINITION;
				cur->tok_class = error;
				cur->tok_value = INVALID;
			}
		}
	}
	else
	{
		// Delete with no WHERE condition
		// TODO: Possible do error checking to make sure correct command
		printf("Delete with NO WHERE condition\n");
		if ((fhandle = fopen(filename, "rbc")) == NULL)
		{
			printf("Error while opening %s file\n", filename);
			rc = FILE_OPEN_ERROR;
			cur->tok_value = INVALID;
		}
		else
		{
			// Read the old header information from the tab file.
			old_header = (table_file_header *)calloc(1, sizeof(table_file_header));
			fread(old_header, sizeof(table_file_header), 1, fhandle);
			fclose(fhandle);
			old_header->file_size = old_header->file_size - (old_header->num_records * old_header->record_size);
			old_header->num_records = 0;
			if ((fhandle = fopen(filename, "wbc")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}
			else
			{
				new_header = (table_file_header *)calloc(1, sizeof(table_file_header));
				memcpy((void *)(new_header), (void *)(old_header), sizeof(table_file_header));
				fwrite(new_header, sizeof(table_file_header), 1, fhandle);
				printf("Delete Sucess! Number of rows affected (%d)", new_header->num_records);
			}
		}
	}
	return rc;
}

int sem_update(token_list *t_list)
{
	int rc = 0;
	printf("PREPARING TO UPDATE\n");
	token_list *cur;
	cur = t_list;
	table_file_header *old_header = NULL;
	table_file_header *new_header = NULL;
	FILE *fhandle = NULL;
	tpd_entry *tab_entry = NULL;
	cd_entry *col_entry = NULL;
	char column_names[MAX_NUM_COL][MAX_IDENT_LEN];
	char column_length[MAX_NUM_COL][MAX_IDENT_LEN];
	char column_type[MAX_NUM_COL][MAX_IDENT_LEN];
	struct stat file_stat;
	char filename[MAX_IDENT_LEN + 4];
	char tablename[MAX_IDENT_LEN + 1];
	char column_to_update[MAX_IDENT_LEN];
	int update_val_int = NULL;
	char update_val_string[MAX_IDENT_LEN];
	int i;
	char cond_column_name[MAX_IDENT_LEN];
	int cond_val = NULL;
	char cond_string[MAX_IDENT_LEN]; // may need to adjust size
	char *records = NULL;
	int records_to_update = 0;
	int records_to_update_indexes[100];
	int index2 = 0;
	int update_column_offset = 0;
	bool update_column_exists = false;
	bool compare_column_exists = false;
	bool column_update_is_int = false;
	bool column_update_is_string = false;
	bool column_update_is_null = false;
	bool column_compare_is_int = false;
	bool column_compare_is_string = false;
	strcpy(tablename, cur->tok_string);
	strcpy(filename, strcat(cur->tok_string, ".tab"));
	if ((tab_entry = get_tpd_from_list(tablename)) == NULL)
	{
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;
		if (cur->tok_value != K_SET)
		{
			rc = INVALID_UPDATE_DEFINITION;
			cur->tok_class = error;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			strcpy(column_to_update, cur->tok_string);
			// tab_entry = get_tpd_from_list(tablename);
			for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
			{
				strcpy(column_names[i], col_entry->col_name);
				sprintf(column_length[i], "%d", col_entry->col_len);
				sprintf(column_type[i], "%d", col_entry->col_type);
			}
			for (i = 0; i < tab_entry->num_columns; i++)
			{
				if (strcmp(column_to_update, column_names[i]) == 0)
				{
					update_column_exists = true;
				}
			}
			if (update_column_exists)
			{
				cur = cur->next;
				if (cur->tok_value != S_EQUAL)
				{
					rc = INVALID_UPDATE_DEFINITION;
					cur->tok_class = error;
					cur->tok_value = INVALID;
				}
				else
				{
					cur = cur->next;
					if (cur->tok_value == INT_LITERAL)
					{
						update_val_int = atoi(cur->tok_string);
						cur = cur->next;
						if (cur->tok_value == K_WHERE)
						{
							cur = cur->next;
							// UPDATE WHERE condition
							strcpy(cond_column_name, cur->tok_string);
							cur = cur->next;
							if (cur->tok_value == S_EQUAL)
							{
								cur = cur->next;
								if (cur->next->tok_value != EOC)
								{
									rc = INVALID_UPDATE_DEFINITION;
									cur->tok_class = error;
									cur->tok_value = INVALID;
								}
								else
								{
									// proceed with update where column = value
									if (cur->tok_value == INT_LITERAL)
									{
										cond_val = atoi(cur->tok_string);
										// command looks good, proceed
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if (atoi(column_type[i]) == T_INT)
													{
														column_compare_is_int = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if (atoi(column_type[i]) == T_INT)
													{
														column_update_is_int = true;
													}
												}
											}
											if (column_update_is_int && compare_column_exists && column_compare_is_int)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1;
															if (atoi(column_type[j]) == T_INT)
															{
																value = (char *)calloc(1, sizeof(int));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																columnOffset += sizeof(int);
																char hexValue[16];
																char temp[16];
																memset(hexValue, '\0', 16);
																strcat(hexValue, "0x");
																sprintf(temp, "%x", (int)value[1]);
																strcat(hexValue, temp);
																sprintf(temp, "%x", (int)value[0]);
																strcat(hexValue, temp);
																long decimal = strtol(hexValue, NULL, 16);
																if (cond_val == decimal)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1);
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc;
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													index2 = 0; // reset
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															columnOffset += 1;
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																if (atoi(column_type[j]) == T_INT)
																{
																	value = (char *)calloc(1, sizeof(int));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																	char hexValue[16];
																	char temp[16];
																	memset(hexValue, '\0', 16);
																	strcat(hexValue, "0x");
																	sprintf(temp, "%x", (int)value[1]);
																	strcat(hexValue, temp);
																	sprintf(temp, "%x", (int)value[0]);
																	strcat(hexValue, temp);
																	long decimal = strtol(hexValue, NULL, 16);
																	if (cond_val == decimal)
																	{
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)&update_val_int), sizeof(int));
																	}
																}
															}
															else
															{
																columnOffset += atoi(column_length[j]);
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
												}

												if ((fhandle = fopen(filename, "wbc")) == NULL)
												{
													rc = FILE_OPEN_ERROR;
												}
												else
												{
													// write file
													fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
													printf("Update success! Number of rows affected (%d)\n", records_to_update);
													free(old_header);
													free(new_header);
													fclose(fhandle);
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else if (cur->tok_value == STRING_LITERAL)
									{
										strcpy(cond_string, cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											// get table information
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_compare_is_string = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if (atoi(column_type[i]) == T_INT)
													{
														column_update_is_int = true;
													}
												}
											}
											if (column_update_is_int && compare_column_exists && column_compare_is_string)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1; // account for length byte
															if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
															{
																value = (char *)calloc(1, atoi(column_length[j]));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																columnOffset += atoi(column_length[j]);
																if (strcmp(cond_string, value) == 0)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1); // account for length byte
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}

												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc;
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																columnOffset += 1;

																if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
																{
																	value = (char *)calloc(1, atoi(column_length[j]));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																	if (strcmp(cond_string, value) == 0)
																	{
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)&update_val_int), sizeof(int));
																	}
																}
															}
															else
															{
																columnOffset += (atoi(column_length[j]) + 1); // account for length byte
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
												}
												if ((fhandle = fopen(filename, "wbc")) == NULL)
												{
													rc = FILE_OPEN_ERROR;
												}
												else
												{
													// write file
													fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
													printf("Update success! Number of rows affected (%d)\n", records_to_update);
													free(old_header);
													free(new_header);
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else
									{
										rc = INVALID_UPDATE_DEFINITION;
										cur->tok_class = error;
										cur->tok_value = INVALID;
									}
								}
							}
							else if (cur->tok_value == S_LESS)
							{
								cur = cur->next;
								if (cur->next->tok_value != EOC)
								{
									rc = INVALID_UPDATE_DEFINITION;
									cur->tok_class = error;
									cur->tok_value = INVALID;
								}
								else
								{
									// proceed with update where column < value
									if (cur->tok_value == INT_LITERAL)
									{
										cond_val = atoi(cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if (atoi(column_type[i]) == T_INT)
													{
														column_compare_is_int = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if (atoi(column_type[i]) == T_INT)
													{
														column_update_is_int = true;
													}
												}
											}
											if (column_update_is_int && compare_column_exists && column_compare_is_int)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														// columnOffset += 1;
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1;
															if (atoi(column_type[j]) == T_INT)
															{
																value = (char *)calloc(1, sizeof(int));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																columnOffset += sizeof(int);
																char hexValue[16];
																char temp[16];
																memset(hexValue, '\0', 16);
																strcat(hexValue, "0x");
																sprintf(temp, "%x", (int)value[1]);
																strcat(hexValue, temp);
																sprintf(temp, "%x", (int)value[0]);
																strcat(hexValue, temp);
																long decimal = strtol(hexValue, NULL, 16);
																if (decimal < cond_val)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1);
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc;
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													index2 = 0; // reset
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															columnOffset += 1;
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																if (atoi(column_type[j]) == T_INT)
																{
																	value = (char *)calloc(1, sizeof(int));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																	char hexValue[16];
																	char temp[16];
																	memset(hexValue, '\0', 16);
																	strcat(hexValue, "0x");
																	sprintf(temp, "%x", (int)value[1]);
																	strcat(hexValue, temp);
																	sprintf(temp, "%x", (int)value[0]);
																	strcat(hexValue, temp);
																	long decimal = strtol(hexValue, NULL, 16);
																	if (decimal < cond_val)
																	{
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)&update_val_int), sizeof(int));
																	}
																}
															}
															else
															{
																columnOffset += atoi(column_length[j]);
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
												}

												if ((fhandle = fopen(filename, "wbc")) == NULL)
												{
													rc = FILE_OPEN_ERROR;
												}
												else
												{
													// write file
													fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
													printf("Update success! Number of records affected (%d)\n", records_to_update);
													free(old_header);
													free(new_header);
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else if (cur->tok_value == STRING_LITERAL)
									{
										strcpy(cond_string, cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_compare_is_string = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if (atoi(column_type[i]) == T_INT)
													{
														column_update_is_int = true;
													}
												}
											}
											if (column_update_is_int && compare_column_exists && column_compare_is_string)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1; // account for length byte
															if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
															{
																value = (char *)calloc(1, atoi(column_length[j]));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																columnOffset += atoi(column_length[j]);
																if (strcmp(value, cond_string) < 0)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1); // account for length byte
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc; // don't proceed further
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																columnOffset += 1;

																if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
																{
																	value = (char *)calloc(1, atoi(column_length[j]));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																	if (strcmp(value, cond_string) < 0)
																	{
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)&update_val_int), sizeof(int));
																	}
																}
															}
															else
															{
																columnOffset += (atoi(column_length[j]) + 1); // account for length byte
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
												}
												if ((fhandle = fopen(filename, "wbc")) == NULL)
												{
													rc = FILE_OPEN_ERROR;
												}
												else
												{
													// write file
													fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
													printf("Update success! Number of rows affected (%d)\n", records_to_update);
													free(old_header);
													free(new_header);
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else
									{
										rc = INVALID_UPDATE_DEFINITION;
										cur->tok_class = error;
										cur->tok_value = INVALID;
									}
								}
							}
							else if (cur->tok_value == S_GREATER)
							{
								cur = cur->next;
								if (cur->next->tok_value != EOC)
								{
									rc = INVALID_UPDATE_DEFINITION;
									cur->tok_class = error;
									cur->tok_value = INVALID;
								}
								else
								{
									// proceed with update where column > value
									if (cur->tok_value == INT_LITERAL)
									{
										cond_val = atoi(cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if (atoi(column_type[i]) == T_INT)
													{
														column_compare_is_int = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if (atoi(column_type[i]) == T_INT)
													{
														column_update_is_int = true;
													}
												}
											}
											if (column_update_is_int && compare_column_exists && column_compare_is_int)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1;
															if (atoi(column_type[j]) == T_INT)
															{
																value = (char *)calloc(1, sizeof(int));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																columnOffset += sizeof(int);
																char hexValue[16];
																char temp[16];
																memset(hexValue, '\0', 16);
																strcat(hexValue, "0x");
																sprintf(temp, "%x", (int)value[1]);
																strcat(hexValue, temp);
																sprintf(temp, "%x", (int)value[0]);
																strcat(hexValue, temp);
																long decimal = strtol(hexValue, NULL, 16);
																if (decimal > cond_val)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1);
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc;
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													index2 = 0; // reset
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															columnOffset += 1;
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																if (atoi(column_type[j]) == T_INT)
																{
																	value = (char *)calloc(1, sizeof(int));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																	char hexValue[16];
																	char temp[16];
																	memset(hexValue, '\0', 16);
																	strcat(hexValue, "0x");
																	sprintf(temp, "%x", (int)value[1]);
																	strcat(hexValue, temp);
																	sprintf(temp, "%x", (int)value[0]);
																	strcat(hexValue, temp);
																	long decimal = strtol(hexValue, NULL, 16);
																	if (decimal > cond_val)
																	{
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)&update_val_int), sizeof(int));
																	}
																}
															}
															else
															{
																columnOffset += atoi(column_length[j]);
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
												}

												if ((fhandle = fopen(filename, "wbc")) == NULL)
												{
													rc = FILE_OPEN_ERROR;
												}
												else
												{
													// write file
													fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
													printf("Update success! Number of records affected (%d)\n", records_to_update);
													free(old_header);
													free(new_header);
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else if (cur->tok_value == STRING_LITERAL)
									{
										strcpy(cond_string, cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_compare_is_string = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if (atoi(column_type[i]) == T_INT)
													{
														column_update_is_int = true;
													}
												}
											}
											if (column_update_is_int && compare_column_exists && column_compare_is_string)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1; // account for length byte
															if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
															{
																value = (char *)calloc(1, atoi(column_length[j]));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																columnOffset += atoi(column_length[j]);
																if (strcmp(value, cond_string) > 0)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1); // account for length byte
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}

												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc; // don't proceed further
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																columnOffset += 1;

																if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
																{
																	value = (char *)calloc(1, atoi(column_length[j]));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																	if (strcmp(value, cond_string) > 0)
																	{
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)&update_val_int), sizeof(int));
																	}
																}
															}
															else
															{
																columnOffset += (atoi(column_length[j]) + 1); // account for length byte
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
												}
												if ((fhandle = fopen(filename, "wbc")) == NULL)
												{
													rc = FILE_OPEN_ERROR;
												}
												else
												{
													// write file
													fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
													printf("Update success! Number of rows affected (%d)\n", records_to_update);
													free(old_header);
													free(new_header);
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else
									{
										rc = INVALID_UPDATE_DEFINITION;
										cur->tok_class = error;
										cur->tok_value = INVALID;
									}
								}
							}
							else
							{
								rc = INVALID_UPDATE_DEFINITION;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
						}
						else
						{
							if (cur->tok_value != EOC)
							{
								rc = INVALID_UPDATE_DEFINITION;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
							else
							{
								// UPDATE no WHERE condition for SET column = <INT LITERAL>
								printf("Everything looks good. starting update w/o where\n");
								printf("File: %s\n", filename);
								if ((fhandle = fopen(filename, "rbc")) == NULL)
								{
									printf("Error while opening %s file\n", filename);
									rc = FILE_OPEN_ERROR;
									cur->tok_value = INVALID;
								}
								else
								{
									// Read the old header information from the tab file.
									fstat(fileno(fhandle), &file_stat);
									old_header = (table_file_header *)calloc(1, file_stat.st_size);
									fread(old_header, file_stat.st_size, 1, fhandle);
									tab_entry = get_tpd_from_list(tablename);
									for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
									{
										strcpy(column_names[i], col_entry->col_name);
										sprintf(column_length[i], "%d", col_entry->col_len);
										sprintf(column_type[i], "%d", col_entry->col_type);
									}
									for (i = 0; i < tab_entry->num_columns; i++)
									{
										printf("\nColumn name: %s, ", column_names[i]);
										printf("Column length: %s, ", column_length[i]);
										printf("Column type: %s \n", column_type[i]);
									}
									int j = 0;
									int columnOffset = 0;
									new_header = (table_file_header *)calloc(1, file_stat.st_size);
									memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
									for (i = 0; i < old_header->num_records; i++)
									{
										while (j < tab_entry->num_columns)
										{
											columnOffset += 1;
											if (strcmp(column_to_update, column_names[j]) == 0)
											{

												if (atoi(column_type[j]) == T_INT)
												{
													memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + columnOffset), (void *)((char *)&update_val_int), sizeof(int));
												}
											}
											else
											{
												columnOffset += atoi(column_length[j]);
											}
											j++;
										}
										// reset
										j = 0;
										columnOffset = 0;
									}
								}
								if ((fhandle = fopen(filename, "wbc")) == NULL)
								{
									rc = FILE_OPEN_ERROR;
								}
								else
								{
									// write file
									fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
									printf("Update success! All rows affected\n");
									free(old_header);
									free(new_header);
								}
							}
						}
					}
					else if (cur->tok_value == STRING_LITERAL)
					{
						strcpy(update_val_string, cur->tok_string);
						cur = cur->next;
						if (cur->tok_value == K_WHERE)
						{
							cur = cur->next;
							// UPDATE WHERE condition
							strcpy(cond_column_name, cur->tok_string);
							cur = cur->next;
							if (cur->tok_value == S_EQUAL)
							{
								cur = cur->next;
								if (cur->next->tok_value != EOC)
								{
									rc = INVALID_UPDATE_DEFINITION;
									cur->tok_class = error;
									cur->tok_value = INVALID;
								}
								else
								{
									if (cur->tok_value == INT_LITERAL)
									{
										cond_val = atoi(cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if (atoi(column_type[i]) == T_INT)
													{
														column_compare_is_int = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_update_is_string = true;
													}
												}
											}
											if (column_update_is_string && compare_column_exists && column_compare_is_int)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
															index2 = j;
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1;
															if (atoi(column_type[j]) == T_INT)
															{
																value = (char *)calloc(1, sizeof(int));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																columnOffset += sizeof(int);
																char hexValue[16];
																char temp[16];
																memset(hexValue, '\0', 16);
																strcat(hexValue, "0x");
																sprintf(temp, "%x", (int)value[1]);
																strcat(hexValue, temp);
																sprintf(temp, "%x", (int)value[0]);
																strcat(hexValue, temp);
																long decimal = strtol(hexValue, NULL, 16);
																if (decimal == cond_val)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1);
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc;
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													int len_updated_value = strlen(update_val_string);
													char *newColumnValueBuffer;
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																columnOffset += 1;
																newColumnValueBuffer = (char *)calloc(1, atoi(column_length[index2]) + 1);
																if (atoi(column_type[j]) == T_INT)
																{
																	value = (char *)calloc(1, sizeof(int));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																	char hexValue[16];
																	char temp[16];
																	memset(hexValue, '\0', 16);
																	strcat(hexValue, "0x");
																	sprintf(temp, "%x", (int)value[1]);
																	strcat(hexValue, temp);
																	sprintf(temp, "%x", (int)value[0]);
																	strcat(hexValue, temp);
																	long decimal = strtol(hexValue, NULL, 16);
																	if (decimal == cond_val)
																	{
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)newColumnValueBuffer), atoi(column_length[index2]) + 1);
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)&len_updated_value), 1);
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)update_val_string), len_updated_value);
																	}
																}
															}
															else
															{
																columnOffset += (atoi(column_length[j]) + 1);
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
													// write to file
													if ((fhandle = fopen(filename, "wbc")) == NULL)
													{
														rc = FILE_OPEN_ERROR;
													}
													else
													{
														// write file
														fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
														printf("Update success! Number of records affected (%d)\n", records_to_update);
														free(old_header);
														free(new_header);
													}
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else if (cur->tok_value == STRING_LITERAL)
									{
										strcpy(cond_string, cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_compare_is_string = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_update_is_string = true;
													}
												}
											}
											if (column_update_is_string && compare_column_exists && column_compare_is_string)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
															index2 = j;
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1; // account for length byte
															if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
															{
																value = (char *)calloc(1, atoi(column_length[j]));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																columnOffset += atoi(column_length[j]);
																if (strcmp(value, cond_string) == 0)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1); // account for length byte
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc; // don't proceed further
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													int len_updated_value = strlen(update_val_string);
													char *newColumnValueBuffer;
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																columnOffset += 1;
																newColumnValueBuffer = (char *)calloc(1, atoi(column_length[index2]) + 1); // include length byte
																if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
																{
																	value = (char *)calloc(1, atoi(column_length[j]));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																	if (strcmp(value, cond_string) == 0)
																	{
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)newColumnValueBuffer), atoi(column_length[index2]) + 1);
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)&len_updated_value), 1);
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)update_val_string), len_updated_value);
																	}
																}
															}
															else
															{
																columnOffset += (atoi(column_length[j]) + 1); // account for length byte
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
												}
												// write to file
												if ((fhandle = fopen(filename, "wbc")) == NULL)
												{
													rc = FILE_OPEN_ERROR;
												}
												else
												{
													// write file
													fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
													printf("Update success! Number of rows affected (%d)\n", records_to_update);
													free(old_header);
													free(new_header);
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else
									{
										rc = INVALID_UPDATE_DEFINITION;
										cur->tok_class = error;
										cur->tok_value = INVALID;
									}
								}
							}
							else if (cur->tok_value == S_LESS)
							{
								cur = cur->next;
								if (cur->next->tok_value != EOC)
								{
									rc = INVALID_UPDATE_DEFINITION;
									cur->tok_class = error;
									cur->tok_value = INVALID;
								}
								else
								{
									if (cur->tok_value == INT_LITERAL)
									{
										cond_val = atoi(cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if (atoi(column_type[i]) == T_INT)
													{
														column_compare_is_int = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_update_is_string = true;
													}
												}
											}
											if (column_update_is_string && compare_column_exists && column_compare_is_int)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
															index2 = j;
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1;
															if (atoi(column_type[j]) == T_INT)
															{
																value = (char *)calloc(1, sizeof(int));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																columnOffset += sizeof(int);
																char hexValue[16];
																char temp[16];
																memset(hexValue, '\0', 16);
																strcat(hexValue, "0x");
																sprintf(temp, "%x", (int)value[1]);
																strcat(hexValue, temp);
																sprintf(temp, "%x", (int)value[0]);
																strcat(hexValue, temp);
																long decimal = strtol(hexValue, NULL, 16);
																if (decimal < cond_val)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1);
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc;
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													int len_updated_value = strlen(update_val_string);
													char *newColumnValueBuffer;
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																columnOffset += 1;
																newColumnValueBuffer = (char *)calloc(1, atoi(column_length[index2]) + 1);
																if (atoi(column_type[j]) == T_INT)
																{
																	value = (char *)calloc(1, sizeof(int));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																	char hexValue[16];
																	char temp[16];
																	memset(hexValue, '\0', 16);
																	strcat(hexValue, "0x");
																	sprintf(temp, "%x", (int)value[1]);
																	strcat(hexValue, temp);
																	sprintf(temp, "%x", (int)value[0]);
																	strcat(hexValue, temp);
																	long decimal = strtol(hexValue, NULL, 16);
																	if (decimal < cond_val)
																	{
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)newColumnValueBuffer), atoi(column_length[index2]) + 1);
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)&len_updated_value), 1);
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)update_val_string), len_updated_value);
																	}
																}
															}
															else
															{
																columnOffset += (atoi(column_length[j]) + 1);
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
													// write to file
													if ((fhandle = fopen(filename, "wbc")) == NULL)
													{
														rc = FILE_OPEN_ERROR;
													}
													else
													{
														// write file
														fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
														printf("Update success! Number of records affected (%d)\n", records_to_update);
														free(old_header);
														free(new_header);
													}
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else if (cur->tok_value == STRING_LITERAL)
									{
										strcpy(cond_string, cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_compare_is_string = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_update_is_string = true;
													}
												}
											}
											if (column_update_is_string && compare_column_exists && column_compare_is_string)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
															index2 = j;
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1; // account for length byte
															if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
															{
																value = (char *)calloc(1, atoi(column_length[j]));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																columnOffset += atoi(column_length[j]);
																if (strcmp(value, cond_string) < 0)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1); // account for length byte
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc; // don't proceed further
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													int len_updated_value = strlen(update_val_string);
													char *newColumnValueBuffer;
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																columnOffset += 1;
																newColumnValueBuffer = (char *)calloc(1, atoi(column_length[index2]) + 1);
																if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
																{
																	value = (char *)calloc(1, atoi(column_length[j]));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																	if (strcmp(value, cond_string) < 0)
																	{
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)newColumnValueBuffer), atoi(column_length[index2]) + 1);
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)&len_updated_value), 1);
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)update_val_string), len_updated_value);
																	}
																}
															}
															else
															{
																columnOffset += (atoi(column_length[j]) + 1); // account for length byte
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
												}
												// write to file
												if ((fhandle = fopen(filename, "wbc")) == NULL)
												{
													rc = FILE_OPEN_ERROR;
												}
												else
												{
													// write file
													fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
													printf("Update success! Number of rows affected (%d)\n", records_to_update);
													free(old_header);
													free(new_header);
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else
									{
										rc = INVALID_UPDATE_DEFINITION;
										cur->tok_class = error;
										cur->tok_value = INVALID;
									}
								}
							}
							else if (cur->tok_value == S_GREATER)
							{
								cur = cur->next;
								if (cur->next->tok_value != EOC)
								{
									rc = INVALID_UPDATE_DEFINITION;
									cur->tok_class = error;
									cur->tok_value = INVALID;
								}
								else
								{
									if (cur->tok_value == INT_LITERAL)
									{
										cond_val = atoi(cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											// get table information
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if (atoi(column_type[i]) == T_INT)
													{
														column_compare_is_int = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_update_is_string = true;
													}
												}
											}
											if (column_update_is_string && compare_column_exists && column_compare_is_int)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
															index2 = j;
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1;
															if (atoi(column_type[j]) == T_INT)
															{
																value = (char *)calloc(1, sizeof(int));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																columnOffset += sizeof(int);
																char hexValue[16];
																char temp[16];
																memset(hexValue, '\0', 16);
																strcat(hexValue, "0x");
																sprintf(temp, "%x", (int)value[1]);
																strcat(hexValue, temp);
																sprintf(temp, "%x", (int)value[0]);
																strcat(hexValue, temp);
																long decimal = strtol(hexValue, NULL, 16);
																if (decimal > cond_val)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1);
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc;
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													int len_updated_value = strlen(update_val_string);
													char *newColumnValueBuffer;
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																columnOffset += 1;
																newColumnValueBuffer = (char *)calloc(1, atoi(column_length[index2]) + 1);
																if (atoi(column_type[j]) == T_INT)
																{
																	value = (char *)calloc(1, sizeof(int));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), sizeof(int));
																	char hexValue[16];
																	char temp[16];
																	memset(hexValue, '\0', 16);
																	strcat(hexValue, "0x");
																	sprintf(temp, "%x", (int)value[1]);
																	strcat(hexValue, temp);
																	sprintf(temp, "%x", (int)value[0]);
																	strcat(hexValue, temp);
																	long decimal = strtol(hexValue, NULL, 16);
																	if (decimal > cond_val)
																	{
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)newColumnValueBuffer), atoi(column_length[index2]) + 1);
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)&len_updated_value), 1);
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)update_val_string), len_updated_value);
																	}
																}
															}
															else
															{
																columnOffset += (atoi(column_length[j]) + 1);
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
													// write to file
													if ((fhandle = fopen(filename, "wbc")) == NULL)
													{
														rc = FILE_OPEN_ERROR;
													}
													else
													{
														// write file
														fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
														printf("Update success! Number of records affected (%d)\n", records_to_update);
														free(old_header);
														free(new_header);
													}
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else if (cur->tok_value == STRING_LITERAL)
									{
										strcpy(cond_string, cur->tok_string);
										if ((fhandle = fopen(filename, "rbc")) == NULL)
										{
											printf("Error while opening %s file\n", filename);
											rc = FILE_OPEN_ERROR;
											cur->tok_value = INVALID;
										}
										else
										{
											// Read the old header information from the tab file.
											fstat(fileno(fhandle), &file_stat);
											old_header = (table_file_header *)calloc(1, file_stat.st_size);
											fread(old_header, file_stat.st_size, 1, fhandle);
											for (i = 0; i < tab_entry->num_columns; i++)
											{
												if (strcmp(cond_column_name, column_names[i]) == 0)
												{
													compare_column_exists = true;
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_compare_is_string = true;
													}
												}
												if (strcmp(column_to_update, column_names[i]) == 0)
												{
													if ((atoi(column_type[i]) == T_CHAR) || (atoi(column_type[i]) == T_VARCHAR))
													{
														column_update_is_string = true;
													}
												}
											}
											if (column_update_is_string && compare_column_exists && column_compare_is_string)
											{
												// copy records to buffer
												records = (char *)calloc(1, (old_header->num_records * old_header->record_size));
												memcpy((void *)((char *)records), (void *)((char *)old_header + old_header->record_offset), (old_header->num_records * old_header->record_size));
												char *value = NULL;
												int j = 0;
												int columnOffset = 0;
												for (i = 0; i < old_header->num_records; i++)
												{
													while (j < tab_entry->num_columns)
													{
														if (strcmp(column_to_update, column_names[j]) == 0)
														{
															update_column_offset = columnOffset + 1; // account for length byte
															index2 = j;
														}
														if (strcmp(cond_column_name, column_names[j]) == 0)
														{
															columnOffset += 1; // account for length byte
															if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
															{
																value = (char *)calloc(1, atoi(column_length[j]));
																memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																columnOffset += atoi(column_length[j]);
																if (strcmp(value, cond_string) > 0)
																{
																	records_to_update++;
																}
															}
														}
														else
														{
															columnOffset += (atoi(column_length[j]) + 1); // account for length byte
														}
														j++;
													}
													// reset
													j = 0;
													columnOffset = 0;
													value = NULL;
												}
												if (records_to_update == 0)
												{
													printf("Warning! No matching rows found\n");
													return rc; // don't proceed further
												}
												else
												{
													new_header = (table_file_header *)calloc(1, file_stat.st_size);
													memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
													int len_updated_value = strlen(update_val_string);
													char *newColumnValueBuffer;
													for (i = 0; i < old_header->num_records; i++)
													{
														while (j < tab_entry->num_columns)
														{
															if (strcmp(cond_column_name, column_names[j]) == 0)
															{
																columnOffset += 1;
																newColumnValueBuffer = (char *)calloc(1, atoi(column_length[index2]) + 1);
																if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
																{
																	value = (char *)calloc(1, atoi(column_length[j]));
																	memcpy((void *)((char *)value), (void *)((char *)records + (i * old_header->record_size) + columnOffset), atoi(column_length[j]));
																	if (strcmp(value, cond_string) > 0)
																	{
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)newColumnValueBuffer), atoi(column_length[index2]) + 1);
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset - 1), (void *)((char *)&len_updated_value), 1);
																		// update row here
																		memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + update_column_offset), (void *)((char *)update_val_string), len_updated_value);
																	}
																}
															}
															else
															{
																columnOffset += (atoi(column_length[j]) + 1); // account for length byte
															}
															j++;
														}
														// reset
														j = 0;
														columnOffset = 0;
														value = NULL;
													}
												}
												// write to file
												if ((fhandle = fopen(filename, "wbc")) == NULL)
												{
													rc = FILE_OPEN_ERROR;
												}
												else
												{
													// write file
													fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
													printf("Update success! Number of rows affected (%d)\n", records_to_update);
													free(old_header);
													free(new_header);
												}
											}
											else
											{
												rc = COLUMN_NOT_EXIST;
												cur->tok_class = error;
												cur->tok_value = INVALID;
											}
										}
									}
									else
									{
										rc = INVALID_UPDATE_DEFINITION;
										cur->tok_class = error;
										cur->tok_value = INVALID;
									}
								}
							}
							else
							{
								rc = INVALID_UPDATE_DEFINITION;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
						}
						else
						{
							if (cur->tok_value != EOC)
							{
								rc = INVALID_UPDATE_DEFINITION;
								cur->tok_class = error;
								cur->tok_value = INVALID;
							}
							else
							{
								// update w/o where SET column = <STRING LITERAL>
								printf("Everything looks good. starting update w/o where\n");
								if ((fhandle = fopen(filename, "rbc")) == NULL)
								{
									printf("Error while opening %s file\n", filename);
									rc = FILE_OPEN_ERROR;
									cur->tok_value = INVALID;
								}
								else
								{
									// Read the old header information from the tab file.
									fstat(fileno(fhandle), &file_stat);
									old_header = (table_file_header *)calloc(1, file_stat.st_size);
									fread(old_header, file_stat.st_size, 1, fhandle);
									tab_entry = get_tpd_from_list(tablename);
									for (i = 0, col_entry = (cd_entry *)((char *)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
									{
										strcpy(column_names[i], col_entry->col_name);
										sprintf(column_length[i], "%d", col_entry->col_len);
										sprintf(column_type[i], "%d", col_entry->col_type);
									}
									int j = 0;
									int columnOffset = 0;
									new_header = (table_file_header *)calloc(1, file_stat.st_size);
									memcpy((void *)((char *)new_header), (void *)old_header, old_header->file_size);
									int len_updated_value = strlen(update_val_string);
									char *reset_columnBuffer;
									for (i = 0; i < old_header->num_records; i++)
									{
										while (j < tab_entry->num_columns)
										{
											if (strcmp(column_to_update, column_names[j]) == 0)
											{
												reset_columnBuffer = (char *)calloc(1, atoi(column_length[j]) + 1);
												if ((atoi(column_type[j]) == T_CHAR) || (atoi(column_type[j]) == T_VARCHAR))
												{
													memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + columnOffset), (void *)((char *)reset_columnBuffer), atoi(column_length[j]) + 1);
													// add new length byte
													memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + columnOffset), (void *)((char *)&len_updated_value), 1);
													// copy the new string
													memcpy((void *)((char *)new_header + new_header->record_offset + (i * old_header->record_size) + columnOffset + 1), (void *)update_val_string, len_updated_value); // account for length byte
												}
											}
											else
											{
												columnOffset += (atoi(column_length[j]) + 1);
											}
											j++;
										}
										// reset
										j = 0;
										columnOffset = 0;
										free(reset_columnBuffer);
									}
								}
							}
							if ((fhandle = fopen(filename, "wbc")) == NULL)
							{
								rc = FILE_OPEN_ERROR;
							}
							else
							{
								// write file
								fwrite((void *)((char *)new_header), old_header->file_size, 1, fhandle);
								printf("Update success! All rows affected\n");
								free(old_header);
								free(new_header);
							}
						}
					}
					else if (cur->tok_value == K_NULL)
					{
						printf("Update column to be null\n");
					}
					else
					{
						rc = INVALID_UPDATE_DEFINITION;
						cur->tok_class = error;
						cur->tok_value = INVALID;
					}
				}
			}
			else
			{
				rc = COLUMN_NOT_EXIST;
				cur->tok_class = error;
				cur->tok_value = INVALID;
			}
		}
	}
	return rc;
}

void print_separator(int n)
{
	int i = 0;
	int length = n;
	while (i < length)
	{
		if (i == length - 1)
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